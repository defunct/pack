package com.goodworkalan.pack;

import java.io.File;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A container for outstanding <code>Page</code> objects that maps addresses to
 * soft referenced <code>Page</code> objects.
 */
final class Pager implements Pack
{
    /** The file where this pager writes its contents. */
    private final File file;

    /** An file channel opened on the associated file. */
    private final FileChannel fileChannel;

    /**
     * Wrapper interface to file channel to allow for IO error
     * simulation during testing.
     */
    private final Disk disk;
    
    /** Housekeeping information stored at the head of the file. */
    private final Header header;

    /**
     * A map of URIs that identify the addresses of static blocks specified
     * at the creation of the pack.
     */
    private final Map<URI, Long> staticBlocks;

    /**
     * This size of a page.
     */
    private final int pageSize;

    /**
     * Round block allocations to this alignment.
     */
    private final int alignment;

    /**
     * The set of journal header positions stored in a {@link PositionSet} that
     * tracks the availability of a fixed number of header position reference
     * positions and blocks a thread that requests a position when the set is
     * empty.
     */
    private final PositionSet journalHeaders;

    /**
     * The map of weak references to raw pages keyed on the file position of the
     * raw page.
     */
    private final Map<Long, RawPageReference> rawPageByPosition;

    /**
     * The queue of weak references to raw pages keyed on the file position of
     * the raw page that is used to remove mappings from the map of pages by
     * position when the raw pages are collected.
     */
    private final ReferenceQueue<RawPage> queue;

    /** The boundary between address pages and user data pages. */
    private final Boundary userBoundary;
    
    /** The boundary between user data pages and interim data pages. */
    private final Boundary interimBoundary;

    /**
     * A reference to linked list of move nodes used as a prototype for the per
     * mutator move list reference.
     */
    private final MoveLatchList moveLatchList;

    /** A set of address pages with available free addresses. */
    private final SortedSet<Long> addressPages;
    
    /**
     * A set of address pages currently checked out by a mutator to allocate a
     * single address, that have more than one free address available.
     */
    private final Set<Long> returningAddressPages;

    /**
     * A synchronization strategy that prevents addresses that have been freed
     * from overwriting reallocations.
     */
    private final AddressLocker addressLocker;

    /**
     * A table that orders user pages with block space available by the size of
     * bytes remaining. During commit, this table is checked for user pages that
     * can accommodate block allocations. To use user page, the page is removed
     * from the table, so that no other mutator will attempt to use it to write
     * block allocations. Once the commit is complete, all user pages with space
     * remaining are added to the free page by size table, or the free page set
     * if the user page is completely empty.
     */
    private final ByRemainingTable freePageBySize;
    
    /**
     * Map of temporary node addresses to byte buffers containing the address
     * value at the temporary node position.
     */
    private final Map<Long, ByteBuffer> temporaryNodes;
    
    /**
     * Map of temporary block addresses to temporary reference node addreses.
     */
    private final Map<Long, Long> temporaries;

    /**
     * Set of empty user pages. This set is checked for empty pages to store
     * allocations during commit. To reuse an empty user page, the page is
     * removed from the set, so that no other mutator will attempt to use it to
     * write block allocations. Once the commit is complete, all user pages with
     * space remaining are added to the free page by size table, or the free
     * page set if the user page is completely empty.
     */
    private final FreeSet emptyUserPages;

    /**
     * A sorted set of of free interim pages sorted in descending order so that
     * we can quickly obtain the last free interim page within interim page
     * space.
     * <p>
     * This set of free interim pages guards against overwrites by a simple
     * method. If the position is in the set of free interim pages, then it is
     * free, if not it is not free. System pages must be allocated while the
     * move lock is locked for reading, or locked for writing in the case of
     * removing free pages from the start of the interim page area when the user
     * area expands.
     * <p>
     * Question: Can't an interim page allocated from the set of free pages be
     * moved while we are first writing to it?
     * <p>
     * Answer: No, because the moving mutator will have to add the moves to the
     * move list before it can move the pages. Adding to move list requires an
     * exclusive lock on the move list.
     * <p>
     * Remember: Only one mutator can move pages in the interim area at a time.
     */
    private final FreeSet freeInterimPages;

    /**
     * A read/write lock that coordinates rewind of area boundaries and the
     * wilderness.
     * <p>
     * The compact lock locks the entire file exclusively and block any other
     * moves or commits. Ordinary commits can run in parallel so long as blocks
     * are moved forward and not backward in in the file.
     */
    private final ReadWriteLock compactLock;

    /**
     * A mutex to ensure that only one mutator at a time is moving pages in the
     * interim page area.
     */
    private final Object expandMutex;

    /**
     * Create a new pager.
     * 
     * @param file
     *            The file where this pager writes its contents.
     * @param fileChannel
     *            An file channel opened on the associated file.
     * @param disk
     *            Wrapper interface to file channel to allow for IO error
     *            simulation during testing.
     * @param header
     *            Housekeeping information stored at the head of the file.
     * @param staticPages
     *            A map of URIs that identify the addresses of static blocks
     *            specified at the creation of the pack.
     * @param addressPages
     *            A set of address pages with available free addresses.
     * @param userBoundary
     *            The boundary between address pages and user data pages.
     * @param interimBoundary
     *            The boundary between user data pages and interim data pages.
     * @param temporaryNodes
     *            Map of temporary node addresses to byte buffers containing the
     *            address value at the temporary node position.
     */
    public Pager(File file, FileChannel fileChannel, Disk disk, Header header, Map<URI, Long> staticPages, SortedSet<Long> addressPages,
        long userBoundary, long interimBoundary, Map<Long, ByteBuffer> temporaryNodes)
    {
        this.file = file;
        this.fileChannel = fileChannel;
        this.disk = disk;
        this.header = header;
        this.alignment = header.getAlignment();
        this.pageSize = header.getPageSize();
        this.userBoundary = new Boundary(pageSize, userBoundary);
        this.interimBoundary = new Boundary(pageSize, interimBoundary);
        this.rawPageByPosition = new HashMap<Long, RawPageReference>();
        this.freePageBySize = new ByRemainingTable(pageSize, alignment);
        this.staticBlocks = Collections.unmodifiableMap(staticPages);
        this.emptyUserPages = new FreeSet();
        this.freeInterimPages = new FreeSet();
        this.queue = new ReferenceQueue<RawPage>();
        this.compactLock = new ReentrantReadWriteLock();
        this.expandMutex = new Object();
        this.moveLatchList = new MoveLatchList();
        this.journalHeaders = new PositionSet(Pack.FILE_HEADER_SIZE, header.getInternalJournalCount());
        this.addressPages = addressPages;
        this.returningAddressPages = new HashSet<Long>();
        this.temporaryNodes = temporaryNodes;
        this.temporaries = temporaries(temporaryNodes);
        this.addressLocker = new AddressLocker();
    }

    /**
     * Create a map of temporary block addresses to the address of their
     * temporary reference node.
     * 
     * @param temporaryNodes
     *            Map of temporary reference node addresses to byte buffers
     *            containing the address value at the temporary node position.
     * @return A map of temporary block addresses to temporary reference node
     *         addresses.
     */
    private static Map<Long, Long> temporaries(Map<Long, ByteBuffer> temporaryNodes)
    {
        Map<Long, Long> temporaries = new HashMap<Long, Long>();
        for (Map.Entry<Long, ByteBuffer> entry : temporaryNodes.entrySet())
        {
            ByteBuffer bytes = entry.getValue();
            long address = bytes.getLong();
            if (address != 0L)
            {
                temporaries.put(address, entry.getKey());
            }
        }
        return temporaries;
    }
    
    /**
     * Return the file associated with this pack.
     * 
     * @return The pack file.
     */
    public File getFile()
    {
        return file;
    }

    /**
     * Return the file channel open on the underlying file.
     * 
     * @return The open file channel.
     */
    public FileChannel getFileChannel()
    {
        return fileChannel;
    }

    /**
     * Return the disk object used to read and write to the file channel. The
     * disk is an class that can be overridden to generate I/O errors during
     * testing.
     * 
     * @return The disk.
     */
    public Disk getDisk()
    {
        return disk;
    }

    /**
     * Get the size of all underlying pages managed by this pager.
     * 
     * @return The page size.
     */
    public int getPageSize()
    {
        return pageSize;
    }
    
    /**
     * Return the alignment to which all block allocations are rounded.
     * 
     * @return The block alignment.
     */
    public int getAlignment()
    {
        return alignment;
    }
    
    /**
     * Return a map of named pages that maps a URI to the address of a static
     * page.
     * 
     * @return The map of named static pages.
     * 
     * @see com.goodworkalan.pack.Pack#getStaticBlocks()
     */
    public Map<URI, Long> getStaticBlocks()
    {
        return staticBlocks;
    }

    /**
     * Get the set of position headers used to reserve journal headers.
     * 
     * @return The set of position headers.
     */
    public PositionSet getJournalHeaders()
    {
        return journalHeaders;
    }

    /**
     * Return the position of the first address page, which may be offset from
     * an aligned page start, in order to accommodate the header.
     * 
     * @return The first address page position.
     */
    public long getFirstAddressPageStart()
    {
        return header.getFirstAddressPageStart();
    }

    /**
     * Get the boundary between address pages and user data pages.
     *
     * @return The boundary between address pages and user data pages.
     */
    public Boundary getUserBoundary()
    {
        return userBoundary;
    }

    /**
     * Get the boundary between user pages and interim pages.
     *
     * @return The boundary between user pages and interim pages.
     */
    public Boundary getInterimBoundary()
    {
        return interimBoundary;
    }

    /**
     * Return the linked list of move latches used to block pages from reading
     * pages while they are moving. Pages are moved during during commit or
     * address region expansion. To prevent mutators from writing blocks to
     * moving pages, a linked list of move latches are appended to this master
     * linked list of move latches. This list is the per pager list. The move
     * latch lists are appended here and iterators are created using this move
     * latch list.
     * 
     * @return The per pager move latch list.
     */
    public MoveLatchList getMoveLatchList()
    {
        return moveLatchList;
    }

    /**
     * Returns the address locker which will block a reallocation from
     * committing until the commit that freed an address has completed. This
     * prevents the reallocation from being overwritten by playback of the
     * journaled free.
     * 
     * @return The per pack address locker.
     */
    public AddressLocker getAddressLocker()
    {
        return addressLocker;
    }

    /**
     * Return a best fit lookup table of user pages with space remaining for
     * block allocation. This lookup table is used to find destinations for
     * newly allocated user blocks during commit.
     * <p>
     * A user page is used by one mutator commit at a time. Removing the page
     * from this table prevents it from being used by another commit.
     * <p>
     * Removing a page from this set, prevents it from being used by an
     * allocation. Removing a page from the available pages sets is the first
     * step in relocating a page.
     * <p>
     * Note that here is no analgous list of free interim pages by size, since
     * interim block pages are not shared between mutators and they are
     * completely reclaimed at the end of a mutation.
     * 
     * @return The best fit lookup table of user pages.
     */
    public ByRemainingTable getFreePageBySize()
    {
        return freePageBySize;
    }

    /**
     * Return the set of completely empty user pages available for block
     * allocation. The set returned is a class that not only contains the set of
     * pages available, but will also prevent a page from being returned to the
     * set of free pages, if that page is midst of relocation.
     * <p>
     * A user page is used by one mutator commit at a time. Removing the page
     * from this table prevents it from being used by another commit.
     * <p>
     * Removing a page from this set, prevents it from being used by an
     * destination for allocations or writes. Removing a page from the available
     * pages sets is the first step in relocating a page.
     * 
     * @return The set of free user pages.
     */
    public FreeSet getEmptyUserPages()
    {
        return emptyUserPages;
    }
    
    /**
     * Return the set of completely empty interim pages available for block
     * allocation. The set returned is a class that not only contains the set of
     * pages available, but will also prevent a page from being returned to the
     * set of free pages, if that page is in the midst of relocation.
     * <p>
     * A user page is used by one mutator commit at a time. Removing the page
     * from this table prevents it from being used by another commit.
     * <p>
     * Removing a page from this set, prevents it from being used as an interim
     * page for an allocation or write. Removing a page from the available pages
     * sets is the first step in relocating a page.
     *
     * @return The set of free user pages.
     */
    public FreeSet getFreeInterimPages()
    {
        return freeInterimPages;
    }

    /**
     * Return the per pack mutex used to ensure that only one mutator at a time
     * is moving pages in the interim page area.
     * 
     * @return The user region expand expand mutex.
     */
    public Object getUserExpandMutex()
    {
        return expandMutex;
    }

    /**
     * A read/write lock on the pager used to prevent compaction during rollback
     * or commit. Compaction is not yet implemented, but it is expected to wreck
     * havoc on normal operation, and should be done exclusively.
     * <p>
     * The complact lock is also held exclusively while closing the file, in
     * order to wait for any rollbacks or commits to complete, and to prevent
     * any rollbacks or commits to initiate during close.
     * 
     * @return The compact read/write lock.
     */
    public ReadWriteLock getCompactLock()
    {
        return compactLock;
    }
    
    /**
     * Remove the references to pages that have garbage collected from their
     * reference queue and from the map of raw pages by position. 
     */
    private synchronized void collect()
    {
        RawPageReference pageReference = null;
        while ((pageReference = (RawPageReference) queue.poll()) != null)
        {
            rawPageByPosition.remove(pageReference.getPosition());
        }
    }

    /**
     * Create a <code>Mutator</code> to inspect and alter the contents of this
     * pack.
     * 
     * @return A new mutator.
     */
    public Mutator mutate()
    {
        final PageRecorder pageRecorder = new PageRecorder();
        final MoveLatchIterator moveLatchIterator = getMoveLatchList().newIterator(pageRecorder);
        return moveLatchIterator.mutate(new Guarded<Mutator>()
        {
            public Mutator run(List<MoveLatch> listOfMoveLatches)
            {
                MoveNodeRecorder moveNodeRecorder = new MoveNodeRecorder();
                DirtyPageSet dirtyPages = new DirtyPageSet(Pager.this, 16);
                Journal journal = new Journal(Pager.this, moveNodeRecorder, pageRecorder, dirtyPages);
                return new Mutator(Pager.this, moveLatchIterator, moveNodeRecorder, pageRecorder, journal, dirtyPages);
            }
        });
    }
    
    /**
     * Allocate a new page from the end of the file by extending the length of
     * the file.
     *
     * @return The address of a new page from the end of file.
     */
    private long fromWilderness()
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(pageSize);

        bytes.putLong(0L); // Checksum.
        bytes.putInt(0); // Is system page.

        bytes.clear();

        long position;

        synchronized (disk)
        {
            try
            {
                position = disk.size(fileChannel);
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_SIZE, e);
            }

            try
            {
                disk.write(fileChannel, bytes, position);
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_WRITE, e);
            }

            try
            {
                if (disk.size(fileChannel) % 1024 != 0)
                {
                    throw new PackException(Pack.ERROR_FILE_SIZE);
                }
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_SIZE, e);
            }
        }

        return position;
    }

    /**
     * Add a raw page to the page by position map.
     *
     * @param rawPage The raw page.
     */
    private void addRawPageByPosition(RawPage rawPage)
    {
        RawPageReference intended = new RawPageReference(rawPage, queue);
        RawPageReference existing = rawPageByPosition.get(intended.getPosition());
        if (existing != null)
        {
            existing.enqueue();
            collect();
        }
        rawPageByPosition.put(intended.getPosition(), intended);
    }

    /**
     * Allocate a new interim position that is initialized by the specified page
     * strategy.
     * <p>
     * This method can only be called from within one of the
     * <code>MoveList.mutate</code> methods. A page obtained from the set of
     * free interim pages will not be moved while the move list is locked
     * shared.
     * 
     * @param <T>
     *            The page strategy for the position.
     * @param page
     *            An instance of the page strategy that will initialize the page
     *            at the position.
     * @param dirtyPages
     *            A map of dirty pages.
     * @return A new interim page.
     */
    public <T extends Page> T newInterimPage(T page, DirtyPageSet dirtyPages)
    {
        // We pull from the end of the interim space to take pressure of of
        // the durable pages, which are more than likely multiply in number
        // and move interim pages out of the way. We could change the order
        // of the interim page set, so that we choose free interim pages
        // from the front of the interim page space, if we want to rewind
        // the interim page space and shrink the file more frequently.
    
        long position = getFreeInterimPages().allocate();
    
        // If we do not have a free interim page available, we will obtain
        // create one out of the wilderness.
    
        if (position == 0L)
        {
            position = fromWilderness();
        }
    
        RawPage rawPage = new RawPage(this, position);
    
        page.create(rawPage, dirtyPages);
    
        synchronized (rawPageByPosition)
        {
            addRawPageByPosition(rawPage);
        }
    
        return page;
    }

    /**
     * Return an interim page for use as a move destination.
     * <p>
     * Question: How do we ensure that free interim pages do not slip into the
     * user data page section? That is, how do we ensure that we're not moving
     * an interim page to a spot that also needs to move?
     * <p>
     * Simple. We gather all the pages that need to move first. Then we assign
     * blank pages only to the pages that are in use and need to move. See
     * <code>tryMove</code> for more discussion.
     * 
     * @return A blank position in the interim area that for use as the target
     *         of a move.
     */
    public long newBlankInterimPage()
    {
        long position = getFreeInterimPages().allocate();
        if (position == 0L)
        {
            position = fromWilderness();
        }
        return position;
    }

    /**
     * Get a raw page from the map of pages by position. If the page reference
     * does not exist in the map of pages by position, or if the page reference
     * has been garbage collected, this method returns null.
     * 
     * @param position
     *            The position of the raw page.
     * @return The page currently mapped to the position or null if no page is
     *         mapped.
     */
    private RawPage getRawPageByPosition(long position)
    {
        RawPage page = null;
        RawPageReference chunkReference = rawPageByPosition.get(position);
        if (chunkReference != null)
        {
            page = chunkReference.get();
        }
        return page;
    }

    /**
     * Get a given page implementation of an underlying raw page for the given
     * position. If the page does not exist, the given page instance is used to
     * load the contents of the underlying raw page. Creation of the page is
     * syncrhronized so that all mutators will reference the same instace of
     * <code>Page</code> and <code>RawPage</code>.
     * <p>
     * If the given page class is a subclass of the page instance currenlty
     * mapped to the page position, the given page is used to load the contents
     * of the underlying raw page and the current page instance is replaced with
     * the subclass page instance. This is used to upgrade a relocatable page,
     * to a specific type of relocatable page (journal, user block, or interim
     * block).
     * <p>
     * If the given page class is a superclass of the page instance currently
     * mappsed to the page position, the current page is returned.
     * <p>
     * The page instance is one that is created solely for this invocation of
     * <code>getPage</code>. It a page of the correct type is in the map of
     * pages by position, the given page instance is ignored and left for the
     * garbage collector.
     * <p>
     * The given page class is nothing more than a type token, to cast the page
     * to correct page type, without generating unchecked cast compiler
     * warnings.
     * 
     * @param position
     *            The page position.
     * @param pageClass
     *            A type token indicating the type of page, used to cast the
     *            page.
     * @param page
     *            An instance to used to load the page if the page does not
     *            exist in the page map.
     * @return The page of the given type for the given position.
     */
    public <P extends Page> P getPage(long position, Class<P> pageClass, P page)
    {
        position = (long) Math.floor(position - (position % pageSize));
        RawPage rawPage = new RawPage(this, position);
        synchronized (rawPage)
        {
            RawPage found = null;
            synchronized (rawPageByPosition)
            {
                found = getRawPageByPosition(position);
                if (found == null)
                {
                    addRawPageByPosition(rawPage);
                }
            }
            if (found == null)
            {
                page.load(rawPage);
            }
            else
            {
                rawPage = found;
            }
        }
        synchronized (rawPage)
        {
            if (!page.getClass().isAssignableFrom(rawPage.getPage().getClass()))
            {
                if (!rawPage.getPage().getClass().isAssignableFrom(page.getClass()))
                {
                    throw new IllegalStateException();
                }
                page.load(rawPage);
            }
        }
        return pageClass.cast(rawPage.getPage());
    }

    /**
     * Set the page at the given position in the map of raw pages by position,
     * to the given page class and given page. This method is called after a
     * page has been moved and its type has been changed, from user block page
     * to address page, or from interim page to user block page.
     * 
     * @param position
     *            The page position.
     * @param pageClass
     *            A type token indicating the type of page, used to cast the
     *            page.
     * @param page
     *            An instance to used to load the page if the page does not
     *            exist in the page map.
     * @param dirtyPages
     *            The set of dirty pages.
     * @param extant
     *            If false, the method will assert that the page does not
     *            already exist in the map of pages by position.
     * @return The page given.
     */
    public <P extends Page> P setPage(long position, Class<P> pageClass, P page, DirtyPageSet dirtyPages, boolean extant)
    {
        position =  position / pageSize * pageSize;
        RawPage rawPage = new RawPage(this, position);

        synchronized (rawPageByPosition)
        {
            RawPage existing = removeRawPageByPosition(position);
            if (existing != null)
            {
                if (!extant)
                {
                    throw new IllegalStateException();
                }
                // TODO Not sure about this. Maybe lock on existing?
                synchronized (existing)
                {
                    page.create(existing, dirtyPages);
                }
            }
            else
            {
                page.create(rawPage, dirtyPages);
                addRawPageByPosition(rawPage);
            }
        }

        return pageClass.cast(rawPage.getPage());
    }

    /**
     * Try to get an address page from the set of available address pages or
     * create address pages by moving user pages if there are none available or
     * outstanding.
     * <p>
     * The caller can allocate one and only address from address page returned.
     * It must then return the address page to the pager.
     * 
     * @param lastSelected
     *            The last selected address page, which we will try to return if
     *            available.
     * 
     * @return An address page with at least one free position.
     */
    private AddressPage getOrCreateAddressPage(long lastSelected)
    {
        // Lock on the set of address pages, which protects the set of
        // address pages and the set of returning address pages.
        
        synchronized (addressPages)
        {
            long position = 0L;         // The position of the new address page.

            // If there are no address pages in the pager that have one
            // or more free addresses and there are no address pages
            // outstanding that have two or more free addresses, then we
            // need to allocate more address pages and try again.
            
            if (addressPages.size() == 0 && returningAddressPages.size() == 0)
            {
                // Create a mutator to move the user page immediately
                // following the address page region  to a new user
                // page.

                final Pager pack = this;
                final PageRecorder pageRecorder = new PageRecorder();
                final MoveLatchIterator listOfMoves = getMoveLatchList().newIterator(pageRecorder);
                Mutator mutator = listOfMoves.mutate(new Guarded<Mutator>()
                {
                    public Mutator run(List<MoveLatch> listOfMoveLatches)
                    {
                        MoveNodeRecorder moveNodeRecorder = new MoveNodeRecorder();
                        DirtyPageSet dirtyPages = new DirtyPageSet(pack, 16);
                        Journal journal = new Journal(pack, moveNodeRecorder, pageRecorder, dirtyPages);
                        return new Mutator(pack, listOfMoves, moveNodeRecorder, pageRecorder, journal,dirtyPages);
                    }
                });

                // See the source of Mutator.newAddressPage.
                addressPages.addAll(mutator.newAddressPages(1));
                
                // There are more pages now, return null indicating we need to
                // try again.
                return null;
            }
            else
            {
                // If we can return the last selected address page,
                // let's. It will help with locality of reference.

                if (addressPages.contains(lastSelected))
                {
                    position = lastSelected;
                }
                else if (addressPages.size() != 0)
                {
                    position = addressPages.first();
                }
                else
                {
                    // We are here because our set of returning address
                    // pages is not empty, meaning that a Mutator will
                    // be returning an address page that has some space
                    // left. We need to wait for it.
                    
                    try
                    {
                        addressPages.wait();
                    }
                    catch (InterruptedException e)
                    {
                    }

                    // When it arrives, we'll return null indicating we
                    // need to try again.

                    return null;
                }

                // Remove the address page from the set of address
                // pages available for allocation.

                addressPages.remove(position);
            }

            // Get the address page.

            AddressPage addressPage = getPage(position, AddressPage.class, new AddressPage());

            // If the address page has two or more addresses available,
            // then we add it to the set of returning address pages, the
            // address pages that have space, but are currently in use,
            // so we should wait for them.

            if (addressPage.getFreeCount() > 1)
            {
                returningAddressPages.add(position);
            }

            // Return the address page.

            return addressPage;
        }
    }

    /**
     * Return an address page with one or more free addresses available for
     * allocation. If the given last selected address page has one or more free
     * addresses available for allocation and it is not in use with another
     * mutator, then it is returned, hopefully helping locality of reference.
     * <p>
     * This method will block if all of the available address pages are in use
     * by other mutators, until one of the other address pages returns an
     * address page. If there are fewer address pages than the minium number of
     * available address pages for allocation outstanding, then a new address
     * page is created by expanding the address page region.
     * <p>
     * TODO Specify a minimum address page pool.
     * 
     * @param lastSelected
     *            The address of the last selected address page.
     * @return An address page with one or more free addresses available for
     *         allocation.
     */
    public AddressPage getAddressPage(long lastSelected)
    {
        for (;;)
        {
            AddressPage addressPage = getOrCreateAddressPage(lastSelected);
            if (addressPage != null)
            {
                return addressPage;
            }
        }
    }

    /**
     * Return an address page to the set of address pages with one or more free
     * addresses available for allocation.
     * <p>
     * The set of free address pages is used as the mutex for threads requesting
     * an address page. If the address page is in the set of returning address
     * pages, the address page is added to the set of free address pages and all
     * threads waiting on the set of address page mutex are notified.
     * 
     * @param addressPage
     *            The address page with one or more free addresses available for
     *            allocation.
     */
    public void returnAddressPage(AddressPage addressPage)
    {
        long position = addressPage.getRawPage().getPosition();
        synchronized (addressPages)
        {
            if (returningAddressPages.remove(position))
            {
                addressPages.add(position);
                addressPages.notifyAll();
            }
        }
    }

    /**
     * Create a journal entry that will write a temporary node reference for the
     * given block address. The temporary journal entry is also used to rollback
     * the assignment of a temporary reference node to the temporary block, if
     * should the mutator rollback.
     * <p>
     * This method will assign the given address to a free temporary node
     * reference, If there is no free temporary node reference, it will allocate
     * one by creating a mutator for the sole purpose of extending the list.
     * <p>
     * The list of temporary reference nodes will grow but never shrink. The
     * temporary reference nodes will be reused when temporary blocks are freed.
     * 
     * @param address
     *            The address of the temporary block.
     * 
     * @see Temporary
     */
    public Temporary getTemporary(long address)
    {
        // Synchronize temporary list manipulation on the temporary node map. 
        Temporary temporary = null;
        synchronized (temporaryNodes)
        {
            BUFFERS: for (;;)
            {
                // Find an empty temporary reference node, or failing that,
                // take node of the last temporary reference node in the linked
                // list of temporary reference nodes in order to append a new
                // temporary reference node later.
                Map.Entry<Long, ByteBuffer> last = null;
                for (Map.Entry<Long, ByteBuffer> entry : temporaryNodes.entrySet())
                {
                    ByteBuffer bytes = entry.getValue();
                    if (bytes.getLong(Pack.ADDRESS_SIZE) == 0L)
                    {
                        last = entry;
                    }
                    else if (bytes.getLong(0) == 0L)
                    {
                        temporaries.put(address, entry.getKey());
                        bytes.putLong(0, Long.MAX_VALUE);
                        temporary = new Temporary(address, entry.getKey());
                        break BUFFERS;
                    }
                }
                final Pager pack = this;
                final PageRecorder pageRecorder = new PageRecorder();
                final MoveLatchIterator moveLatches = getMoveLatchList().newIterator(pageRecorder);
                Mutator mutator = moveLatches.mutate(new Guarded<Mutator>()
                {
                    public Mutator run(List<MoveLatch> listOfMoveLatches)
                    {
                        MoveNodeRecorder moveNodeRecorder = new MoveNodeRecorder();
                        DirtyPageSet dirtyPages = new DirtyPageSet(pack, 16);
                        Journal journal = new Journal(pack, moveNodeRecorder, pageRecorder, dirtyPages);
                        return new Mutator(pack, moveLatches, moveNodeRecorder, pageRecorder, journal,dirtyPages);
                    }
                });
                
                long next = mutator.allocate(Pack.ADDRESS_SIZE * 2);
                
                ByteBuffer bytes = mutator.read(next);
                while (bytes.remaining() != 0)
                {
                    bytes.putLong(0L);
                }
                bytes.flip();
                
                mutator.write(next, bytes);

                last.getValue().clear();
                last.getValue().putLong(Pack.ADDRESS_SIZE, next);
                
                mutator.write(last.getKey(), last.getValue());
                
                mutator.commit();
                
                temporaryNodes.put(next, bytes);
            }
        }

        return temporary;
    }

    /**
     * Set the temporary node at the given temporary node position to reference
     * the given block address. This method is used by the temporary journal
     * entry to set the value of the temporary reference node.
     * 
     * @param address
     *            The temporary block address.
     * @param temporary
     *            The address of the temporary reference node.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    public void setTemporary(long address, long temporary, DirtyPageSet dirtyPages)
    {
        // Synchronize temporary list manipulation on the temporary node map. 
        synchronized (temporaryNodes)
        {
            ByteBuffer bytes = temporaryNodes.get(temporary);
            bytes.putLong(0, address);
            bytes.clear();

            // Use the checked address dereference to find the  
            AddressPage addresses = getPage(temporary, AddressPage.class, new AddressPage());
            long lastPosition = 0L;
            for (;;)
            {
                long position = addresses.dereference(temporary);
                if (lastPosition == position)
                {
                    throw new IllegalStateException();
                }
                UserPage user = getPage(position, UserPage.class, new UserPage());
                synchronized (user.getRawPage())
                {
                    if (user.write(temporary, bytes, dirtyPages))
                    {
                        break;
                    }
                }
                lastPosition = position;
            }
        }
    }

    /**
     * Free a temporary block reference, setting the block reference to zero,
     * making it available for use to reference another temporary block.
     * 
     * @param address
     *            The address of the temporary block.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void freeTemporary(long address, DirtyPageSet dirtyPages)
    {
        // Synchronize temporary list manipulation on the temporary node map. 
        synchronized (temporaryNodes)
        {
            Long temporary = temporaries.get(address);
            if (temporary != null)
            {
                setTemporary(0L, temporary, dirtyPages);
            }
        }
    }

    /**
     * Return a temporary block reference to the pool of temporary block
     * references as the result of a rollback of a commit.
     * 
     * @param address
     *            The address of the temporary block.
     * @param temporary
     *            The address of temporary reference node.
     */
    public void rollbackTemporary(long address, long temporary)
    {
        // Synchronize temporary list manipulation on the temporary node map. 
        synchronized (temporaryNodes)
        {
            temporaries.remove(address);
            temporaryNodes.get(temporary).putLong(0, 0L);
        }
    }

    /**
     * Return a user page to the free page accounting, if the page has any 
     * space remaining for blocks. If the block page is empty, it is added
     * to the set of empty user pages. If it has block space remaining that
     * is greater than the alignment, then it is added to by size lookup table.
     * <p>
     * TODO Pull this out and create a pool.
     * 
     * @param userPage The user block page.
     */
    public void returnUserPage(UserPage userPage)
    {
        if (userPage.getCount() == 0)
        {
            emptyUserPages.free(userPage.getRawPage().getPosition());
        }
        else if (userPage.getRemaining() > getAlignment())
        {
            getFreePageBySize().add(userPage);
        }
    }

    /**
     * Remove a raw page from the map of pages by position. If the page exists
     * in the map, The page is completly removed by enqueuing the weak page
     * reference and running <code>collect</code>.
     * 
     * @param position
     *            The position of the raw page to remove.
     * @return The page currently mapped to the position or null if no page is
     *         mapped.
     */
    private RawPage removeRawPageByPosition(long position)
    {
        RawPageReference existing = rawPageByPosition.get(new Long(position));
        RawPage p = null;
        if (existing != null)
        {
            p = existing.get();
            existing.enqueue();
            collect();
        }
        return p;
    }

    /**
     * Relocate a page in the pager by removing it from the map of pages by
     * position at the given from position and adding it at the given to
     * position. This only moves the <code>RawPage</code> in the pager, it does
     * not copy the page to the new position in the underlying file.
     * 
     * @param from
     *            The position to move from.
     * @param to
     *            The position to move to.
     */
    public void relocate(long from, long to)
    {
        synchronized (rawPageByPosition)
        {
            RawPage position = removeRawPageByPosition(from);
            if (position != null)
            {
                assert to == position.getPosition();
                addRawPageByPosition(position);
            }
        }
    }

    /**
     * Return a file position based on the given file position adjusted by the
     * linked list of page moves appended to the given move node. The adjustment
     * will skip the number of move nodes given by skip.
     * <p>
     * The adjustment will account for offset into the page position. This is
     * necessary for next operations in journals, which may jump to any
     * operation in a journal, which may be at any location in a page.
     * 
     * @param moveNode
     *            The head of a list of move nodes.
     * @param position
     *            The file position to track.
     * @param skip
     *            The number of moves to skip (0 or 1).
     * @return The file position adjusted by the recorded page moves.
     */
    public long adjust(MoveNode moveNode, long position, int skip)
    {
        int offset = (int) (position % pageSize);
        position = position - offset;
        while (moveNode.getNext() != null)
        {
            moveNode = moveNode.getNext();
            Move move = moveNode.getMove();
            if (move.getFrom() == position)
            {
                if (skip == 0)
                {
                    position = move.getTo();
                }
                else
                {
                    skip--;
                }
            }
        }
        return position + offset;
    }

    /**
     * Return a file position based on the given file position adjusted by page
     * moves in the given list of move node.
     * <p>
     * The adjustment will account for offset into the page position. This is
     * necessary for next operations in journals, which may jump to any
     * operation in a journal, which may be at any location in a page.
     * 
     * @param moveList
     *            The list of page moves.
     * @param position
     *            The file position to track.
     * @return The file position adjusted by the recorded page moves.
     */
    public long adjust(List<Move> moveList, long position)
    {
        int offset = (int) (position % pageSize);
        position = position - offset;
        for (Move move: moveList)
        {
            if (move.getFrom() == position)
            {
                position = move.getTo();
            }
        }
        return position + offset;
    }
    
    public void copacetic()
    {
    }

    /**
     * Close the pager writing out the region boudnaries and the soft shutdown
     * flag in the header and variable pager state to a region at the end of the
     * file. The variable data includes address pages with addresses remaining,
     * empty user pages, and user pages with space remaining. The variable data
     * is positioned at the location indicated by the user to interim boundary.
     * <p>
     * Close will wait for any concurrent commits to complete.
     */
    public void close()
    {
        // Grab the exclusive compact lock, which will wait for any concurrent
        // commits to complete.

        getCompactLock().writeLock().lock();

        try
        {
            // Write the set of address pages, the set of empty user pages and
            // the set of pages with space remaining to a byte buffer.

            int size = 0;
            
            size += Pack.COUNT_SIZE + addressPages.size() * Pack.POSITION_SIZE;
            size += Pack.COUNT_SIZE + (emptyUserPages.size() + getFreePageBySize().getSize()) * Pack.POSITION_SIZE;
            
            ByteBuffer reopen = ByteBuffer.allocateDirect(size);
            
            reopen.putInt(addressPages.size());
            for (long position: addressPages)
            {
                reopen.putLong(position);
            }
           
            reopen.putInt(emptyUserPages.size() + getFreePageBySize().getSize());
            for (long position: emptyUserPages)
            {
                reopen.putLong(position);
            }
            for (long position: getFreePageBySize())
            {
                reopen.putLong(position);
            }
            
            reopen.flip();

            // Write the variable data at the interim page positions.
            
            try
            {
                disk.write(fileChannel, reopen, getInterimBoundary().getPosition());
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_WRITE, e);
            }

            // Write the boundaries and soft shutdown flag.
            
            try
            {
                disk.truncate(fileChannel, getInterimBoundary().getPosition() + reopen.capacity());
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_TRUNCATE, e);
            }
            
            header.setDataBoundary(getUserBoundary().getPosition());
            header.setOpenBoundary(getInterimBoundary().getPosition());
    
            header.setShutdown(Pack.SOFT_SHUTDOWN);
            try
            {
                header.write(disk, fileChannel);
                disk.close(fileChannel);
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_CLOSE, e);
            }
        }
        finally
        {
            getCompactLock().writeLock().unlock();
        }
    }
}

/* vim: set tw=80 : */
