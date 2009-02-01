package com.goodworkalan.pack;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.goodworkalan.sheaf.Sheaf;

final class Bouquet
{
    private final Pack pack;
    
    /** The page manager of the pack to mutate. */
    private final Sheaf sheaf;
    
    /**
     * A synchronization strategy that prevents addresses that have been freed
     * from overwriting reallocations.
     */
    private final AddressLocker addressLocker;

    /**
     * Round block allocations to this alignment.
     */
    private final int alignment;

    private final TemporaryServer temporaryFactory;
    
    private final AddressPagePool addressPagePool;
    
    private final InterimPagePool interimPagePool;
    
    /** The boundary between user data pages and interim data pages. */
    private final Boundary interimBoundary;

    private final UserPagePool userPagePool;
    
    /**
     * A reference to linked list of move nodes used as a prototype for the per
     * mutator move list reference.
     */
    private final MoveLatchList moveLatchList;

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
     * A map of URIs that identify the addresses of static blocks specified
     * at the creation of the pack.
     */
    private final Map<URI, Long> staticBlocks;

    /**
     * The set of journal header positions stored in a {@link PositionSet} that
     * tracks the availability of a fixed number of header position reference
     * positions and blocks a thread that requests a position when the set is
     * empty.
     */
    private final PositionSet journalHeaders;

    /** The boundary between address pages and user data pages. */
    private final Boundary userBoundary;

    /** The file where this pager writes its contents. */
    private final File file;
    
    /** Housekeeping information stored at the head of the file. */
    private final Header header;

    private final MutatorFactory mutatorFactory;

    /**
     * @param file
     *            The file where this pager writes its contents.
     * @param alignment
     *            The alignment to which all block allocations are rounded.
     * @param sheaf
     *            Page management.
     * @param header
     *            Housekeeping information stored at the head of the file.
     * @param addressPagePool
     * @param interimPagePool
     * @param temporaryFactory
     * @param userPagePool
     * @param staticPages
     *            A map of URIs that identify the addresses of static blocks
     *            specified at the creation of the pack.
     * @param userBoundary
     *            The boundary between address pages and user data pages.
     * @param interimBoundary
     *            The boundary between user data pages and interim data pages.
     */
    public Bouquet(File file, Header header, Map<URI, Long> staticBlocks, long userBoundary, long interimBoundary, Sheaf sheaf, AddressPagePool addressPagePool, TemporaryServer temporaryFactory)
    {
        this.pack = new Pack(this);
        this.file = file;
        this.alignment = header.getAlignment();
        this.header = header;
        this.staticBlocks = staticBlocks;
        this.journalHeaders = new PositionSet(Pack.FILE_HEADER_SIZE, header.getInternalJournalCount());
        this.userBoundary = new Boundary(sheaf.getPageSize(), userBoundary);
        this.interimBoundary = new Boundary(sheaf.getPageSize(), interimBoundary);
        this.sheaf = sheaf;
        this.addressPagePool = addressPagePool;
        this.userPagePool = new UserPagePool(sheaf.getPageSize(), alignment);
        this.interimPagePool = new InterimPagePool();
        this.temporaryFactory = temporaryFactory;
        this.moveLatchList = new MoveLatchList();
        this.compactLock = new ReentrantReadWriteLock();
        this.expandMutex = new Object();
        this.addressLocker = new AddressLocker();
        this.mutatorFactory = new MutatorFactory(this);
    }
    
    public Pack getPack()
    {
        return pack;
    }
    
    public Header getHeader()
    {
        return header;
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
    
    public MutatorFactory getMutatorFactory()
    {
        return mutatorFactory;
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

    public Sheaf getSheaf()
    {
        return sheaf;
    }
    
    public AddressPagePool getAddressPagePool()
    {
        return addressPagePool;
    }

    public InterimPagePool getInterimPagePool()
    {
        return interimPagePool;
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

    public TemporaryServer getTemporaryFactory()
    {
        return temporaryFactory;
    }
    
    public UserPagePool getUserPagePool()
    {
        return userPagePool;
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
     * Get the boundary between user pages and interim pages.
     *
     * @return The boundary between user pages and interim pages.
     */
    public Boundary getInterimBoundary()
    {
        return interimBoundary;
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
        int offset = (int) (position % sheaf.getPageSize());
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
     * Return the file associated with this pack.
     * 
     * @return The pack file.
     */
    public File getFile()
    {
        return file;
    }
}
