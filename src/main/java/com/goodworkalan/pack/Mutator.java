package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Segment;

/**
 * An isolated view of an atomic alteration the contents of a {@link Pack}. In
 * order to allocate, read, write or free blocks, one must create a
 * <code>Mutator</code> by calling {@link Pack#mutate()}.
 */
public final class Mutator
{
    /** The bouquet of sundry services. */
    private final Bouquet bouquet;
    
    /** A journal to record the isolated mutations of the associated pack. */
    private final Journal journal;

    /** A table that orders allocation pages by the size of bytes remaining. */
    private final ByRemainingTable allocByRemaining;
    
    /**
     * A map of addresses to movable position references to the blocks the
     * addresses reference. Address keys stored as negative values indicate that
     * the address was allocated during this mutation, address keys stored as
     * positive values indicate that the address references a write of an
     * existing allocation.
     */
    private final SortedMap<Long, Long> addresses;

    /** A set of pages that need to be flushed to the disk.  */
    private final DirtyPageSet dirtyPages;
    
    /**
     * A list of journal entries that write temporary node references for the
     * temporary block allocations of this mutator.
     */
    private final List<Temporary> temporaries;

    /**
     * The address of the last address page used to allocate an address. We give
     * this to the pager when we request an address page to indicate a
     * preference, in hopes of improving locality of reference.
     * 
     * @see Pager#getAddressPage(long)
     */
    private long lastAddressPage;
    
    /**
     * Create a new mutator to alter the contents of a specific pagers.
     * 
     * @param pager
     *            The page manager of the pack to mutate.
     * @param moveLatchList
     *            A reference to the per pager linked list of latched page
     *            moves.
     * @param moveNodeRecorder
     *            The per mutator recorder of move nodes that appends the page
     *            moves to a linked list of move nodes.
     * @param pageRecorder
     *            The per mutator recorder of move nodes that adjusts the file
     *            positions of referenced pages.
     * @param journal
     *            A journal to record the isolated mutations of the associated
     *            pack.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    Mutator(Bouquet bouquet, Journal journal, DirtyPageSet dirtyPages)
    {
        ByRemainingTable allocByRemaining = new ByRemainingTable(bouquet.getSheaf().getPageSize(), bouquet.getAlignment());

        this.bouquet = bouquet;
        this.journal = journal;
        this.allocByRemaining = allocByRemaining;
        this.dirtyPages = dirtyPages;
        this.addresses = new TreeMap<Long, Long>();
        this.temporaries = new ArrayList<Temporary>();
    }

    /**
     * Return the pack that this mutator alters.
     * 
     * @return The pack.
     */
    public Pack getPack()
    {
        return bouquet.getPack();
    }

    /**
     * Allocate a block whose address will be returned in the list of temporary
     * blocks when the pack is reopened.
     * <p>
     * I've implemented this using user space, which seems to imply that I don't
     * need to provide this as part of the core. I'm going to attempt to
     * implement it as a user object.
     * 
     * @param blockSize
     *            Size of the temporary block to allocate.
     * 
     * @return The address of the block.
     */
    public long temporary(int blockSize)
    {
        long address = allocate(blockSize);
        
        final Temporary temporary = bouquet.getTemporaryFactory().getTemporary(bouquet.getMutatorFactory(), address);

        journal.write(temporary);
        
        temporaries.add(temporary);
        
        return address;
    }

    /**
     * Allocate a block in the <code>Pack</code> to accommodate a block of the
     * specified block size. This method will reserve a new block and return the
     * address of the block. The block will not be visible to other mutators
     * until the mutator commits it's changes.
     * 
     * @param blockSize
     *            The size of the block to allocate.
     * @return The address of the block.
     */
    public long allocate(int blockSize)
    {
        AddressPage addressPage = null;
        final long address;
        addressPage = bouquet.getAddressPagePool().getAddressPage(bouquet.getMutatorFactory(), bouquet.getSheaf(), lastAddressPage);
        try
        {
            address = addressPage.reserve(dirtyPages);
        }
        finally
        {
            bouquet.getAddressPagePool().returnAddressPage(addressPage);
        }
        
        // Add the header size to the block size.
                
        final int fullSize = blockSize + Pack.BLOCK_HEADER_SIZE;
           
        bouquet.getPageMoveLock().readLock().lock();
        try
        {
            // This is unimplemented: Creating a linked list of blocks when
            // the block size exceeds the size of a page.
            
            int pageSize = bouquet.getSheaf().getPageSize();
            if (fullSize + Pack.BLOCK_PAGE_HEADER_SIZE > pageSize)
            {
                // Recurse.
                throw new UnsupportedOperationException();
            }
            
            // If we already have a wilderness data page that will fit the
            // block, use that page. Otherwise, allocate a new wilderness
            // data page for allocation.
    
    
            // We know that our already reserved pages are not moving
            // because our page recorder will wait for them to move.
    
            // We know that the new interim page is not moving because we
            // have a shared lock on all moves, so if it is going to move
            // it will move after this operation. It would not be in the
            // set of free interim pages if it was also scheduled to move.
    
            BlockPage interim = null;
            long bestFit = allocByRemaining.bestFit(fullSize);
            if (bestFit == 0L)
            {
                interim = bouquet.getInterimPagePool().newInterimPage(bouquet.getSheaf(), BlockPage.class, new BlockPage(), dirtyPages);
            }
            else
            {
                interim = bouquet.getSheaf().getPage(bestFit, BlockPage.class, new BlockPage());
            }
            
            // Allocate a block from the wilderness data page.
            
            interim.allocate(address, fullSize, dirtyPages);
            
            allocByRemaining.add(interim);
            
            addresses.put(-address, interim.getRawPage().getPosition());
            
            return address;
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }
    }

    /**
     * Write the remaining bytes of the given source buffer to the block
     * referenced by the given block address. If there are more bytes remaining
     * in the source buffer than the size of the addressed block, no bytes are
     * transferred and a <code>BufferOverflowException</code> is thrown.
     * 
     * @param address
     *            The block address.
     * @param source
     *            The buffer whose remaining bytes are written to the block
     *            referenced by the address.
     * 
     * @throws BufferOverflowException
     *             If there is insufficient space in the block for the remaining
     *             bytes in the source buffer.
     */
    public void write(final long address, final ByteBuffer source)
    {
        bouquet.getPageMoveLock().readLock().lock();
        try
        {
            // For now, the first test will write to an allocated block, so
            // the write buffer is already there.
            BlockPage interim = null;
            Long isolated = getIsolated(address);

            if (isolated == null)
            {
                // Interim block pages allocated to store writes are tracked
                // in a separate by size table and a separate set of interim
                // pages. During commit interim write blocks need only be
                // copied to the user pages where they reside, while interim
                // alloc blocks need to be assigned (as a page) to a user
                // page with space to accommodate them.

                int blockSize = 0;
                do
                {
                    BlockPage blocks = bouquet.getUserBoundary().dereference(bouquet.getSheaf(), address);
                    blockSize = blocks.getBlockSize(address);
                }
                while (blockSize == 0);
               
                long bestFit = allocByRemaining.bestFit(blockSize);
                if (bestFit == 0L)
                {
                    interim = bouquet.getInterimPagePool().newInterimPage(bouquet.getSheaf(), BlockPage.class, new BlockPage(), dirtyPages);
                }
                else
                {
                    interim = bouquet.getSheaf().getPage(bestFit, BlockPage.class, new BlockPage());
                }
                
                interim.allocate(address, blockSize, dirtyPages);
                
                if (blockSize < source.remaining() + Pack.BLOCK_HEADER_SIZE)
                {
                    ByteBuffer read = null;
                    ByteBuffer copy = ByteBuffer.allocateDirect(blockSize - Pack.BLOCK_HEADER_SIZE);
                    do
                    {
                        BlockPage blocks = bouquet.getUserBoundary().dereference(bouquet.getSheaf(), address);
                        read = blocks.read(address, copy);
                    }
                    while (read == null);
                    read.flip();
                    interim.write(address, read, dirtyPages);
                }

                addresses.put(address, interim.getRawPage().getPosition());
            }
            else
            {
                interim = bouquet.getUserBoundary().load(bouquet.getSheaf(), isolated, BlockPage.class, new BlockPage());
            }

            if (!interim.write(address, source, dirtyPages))
            {
                throw new IllegalStateException();
            }
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }
    }

    private Long getIsolated(long address)
    {
        Long isolated = addresses.get(address);
        if (isolated == null)
        {
            isolated = addresses.get(-address);
        }
        return isolated;
    }

    /**
     * Read the block at the given address into the a buffer and return the
     * buffer. This method will allocate a byte buffer of the block size.
     * 
     * @param address
     *            The block address.
     * @return A buffer containing the contents of the block.
     */
    public ByteBuffer read(long address)
    {
        ByteBuffer bytes = tryRead(address, null);
        bytes.flip();
        return bytes;
    }

    /**
     * Read the block referenced by the given address into the given destination
     * buffer. If the block size is greater than the bytes remaining in the
     * destination buffer, size of the addressed block, no bytes are transferred
     * and a <code>BufferOverflowException</code> is thrown.
     * 
     * @param address
     *            The address of the block.
     * @param destination
     *            The destination byte buffer.
     * @throws BufferOverflowException
     *             If the size of the block is greater than the bytes remaining
     *             in the destination buffer.
     */
    public void read(long address, ByteBuffer destination)
    {
        if (destination == null)
        {
            throw new NullPointerException();
        }
        tryRead(address, destination);
    }

    /**
     * Read the block at the given address into the given destination buffer. If
     * the given destination buffer is null, this method will allocate a byte
     * buffer of the block size. If the given destination buffer is not null and
     * the block size is greater than the bytes remaining in the destination
     * buffer, size of the addressed block, no bytes are transferred and a
     * <code>BufferOverflowException</code> is thrown.
     * 
     * @param address
     *            The block address.
     * @param destination
     *            The destination buffer or null to indicate that the method
     *            should allocate a destination buffer of block size.
     * @return The given or created destination buffer.
     * @throws BufferOverflowException
     *             If the size of the block is greater than the bytes remaining
     *             in the destination buffer.
     */
    private ByteBuffer tryRead(long address, ByteBuffer destination)
    {
        bouquet.getPageMoveLock().readLock().lock();
        try
        {
            ByteBuffer read = null;
            Long isolated = getIsolated(address);
            if (isolated == null)
            {
                do
                {
                    BlockPage user = bouquet.getUserBoundary().dereference(bouquet.getSheaf(), address);
                    read = user.read(address, destination);
                }
                while (read == null);
            }
            else
            {
                BlockPage interim = bouquet.getUserBoundary().load(bouquet.getSheaf(), isolated, BlockPage.class, new BlockPage());
                read = interim.read(address, destination);
            }
    
            return read;
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }
    }

    /**
     * Free a block in the pack. Free will free a block. The block may have been
     * committed, or it may have been allocated by this mutator. That is, you
     * can free blocks allocated by a mutator, before they are written by a
     * commit or discarded by a rollback.
     * <p>
     * The client programmer is responsible for synchronizing writes and frees.
     * A client program must not free an address that is being freed or written
     * by another mutator.
     * <p>
     * The free is isolated, so that a read of the address of a committed block
     * will still be valid while the free is uncommitted.
     * 
     * @param address
     *            The address of the block to free.
     */
    public void free(final long address)
    {
        // User is not allowed to free named blocks.
        if (bouquet.getStaticBlocks().containsValue(address))
        {
            throw new PackException(Pack.ERROR_FREED_STATIC_ADDRESS);
        }

        // Ensure that no pages move.
        bouquet.getPageMoveLock().readLock().lock();
        try
        {
            // If true, the block was allocated during this mutation and
            // does not require a journaled free.
            boolean unallocate = false;

            // See if the block was allocated during this mutation.  This is
            // indicated by its presence in the address map with the
            // negative value of the address as a key. If not, check to see
            // if there has been a write to the address during this
            // mutation.
            Long isolated = addresses.get(-address);
            if (isolated != null)
            {
                unallocate = true;
            }
            else
            {
                isolated = addresses.get(address);
            }

            // If there is an interim block for the address, we need to free
            // the interim block.
            if (isolated != null)
            {
                // Figure out which by size table contains the page.  We
                // will not reinsert the page if it is not already in the by
                // size table.
                boolean reinsert = allocByRemaining.remove(isolated) != 0;
                
                // Free the block from the interim page.
                BlockPage interim = bouquet.getUserBoundary().load(bouquet.getSheaf(), isolated, BlockPage.class, new BlockPage());
                interim.unallocate(address, dirtyPages);
                
                // We remove and reinsert because if we free from the end of
                // the block, the bytes remaining will change.
                if (reinsert)
                {
                    allocByRemaining.add(interim);
                }
            }

            // If we are freeing a block allocated by this mutator, we
            // simply need to set the file position referenced by the
            // address to zero. Otherwise, we need to write a journaled free
            // to the journal.
            if (unallocate)
            {
                AddressPage addresses = bouquet.getSheaf().getPage(-address, AddressPage.class, new AddressPage());
                addresses.free(address, dirtyPages);
            }
            else
            {
                journal.write(new Free(address));
            }
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }
    }
    
    /**
     * Internal implementation of rollback, guarded by the per pager compact
     * lock and the list of move latches, performs the rollback as described in
     * the public {@link #rollback()} method.
     */
    private void tryRollback()
    {
        Set<Long> interims = new TreeSet<Long>();
        
        // For each address in the isolated address map, if the address is
        // greater than zero, it is an allocation by this mutator, so we need to
        // set the file position referenced by the address to zero.
        for (Map.Entry<Long, Long> entry : addresses.entrySet())
        {
            long address = entry.getKey();
            if (address < 0)
            {
                AddressPage addresses = bouquet.getSheaf().getPage(-address, AddressPage.class, new AddressPage());
                addresses.free(-address, dirtyPages);
                dirtyPages.flushIfAtCapacity();
            }
            interims.add(entry.getValue());
        }

        interims = bouquet.getUserBoundary().adjust(interims);
        
        Set<Long> journalPages = bouquet.getUserBoundary().adjust(journal.getJournalPages());
        
        // Each of the allocations of temporary blocks, blocks that are returned
        // by the opener when the file is reopened, needs to be rolled back. 
        for (Temporary temporary : temporaries)
        {
            temporary.rollback(bouquet.getTemporaryFactory());
        }
        
        // Write any dirty pages.
        dirtyPages.flush();
        
        // Put the interim pages we used back into the set of free interim
        // pages.
        bouquet.getInterimPagePool().getFreeInterimPages().free(interims);
        bouquet.getInterimPagePool().getFreeInterimPages().free(journalPages);
    }
    
    /**
     * Reset this mutator for reuse.
     *
     * @param commit The state of the commit.
     */
    private void clear()
    {
        journal.reset();
        allocByRemaining.clear();
        addresses.clear();
        dirtyPages.clear();
        temporaries.clear();
        lastAddressPage = 0;
    }

    /**
     * Rollback the changes made to the file by this mutator. Rollback will
     * reset the mutator discarding any changes made to the file.
     * <p>
     * After the rollback is complete, the mutator can be reused.
     */
    public void rollback()
    {
        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.
        bouquet.getPageMoveLock().readLock().lock();
        try
        {
            tryRollback();
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }

        clear();
    }

    /**
     * Create address pages, extending the address page region by moving the
     * user pages immediately follow after locking the pager to prevent
     * compaction and close.
     * <p>
     * Remember that this method is already guarded in <code>Pager</code> by
     * synchronizing on the set of free addresses.
     * 
     * @param newAddressPageCount
     *            The number of address pages to allocate.
     */
    private SortedSet<Long> tryNewAddressPage(int newAddressPageCount)
    {
        // TODO These pages that you are creating, are they getting into 
        // the free lists, or are they getting lost?
        
        // The set of newly created address pages.
        SortedSet<Long> newAddressPages = new TreeSet<Long>();
        Set<Long> pagesToMove = new HashSet<Long>();
        Set<Long> interimPages = new HashSet<Long>();
        Set<Long> userPages = new HashSet<Long>();
        
        // Now we know we have enough user pages to accommodate our creation of
        // address pages. That is we have enough user pages, full stop. We have
        // not looked at whether they are free or in use.
        
        // Some of those user block pages may not yet exist. We are going to
        // have to wait until they exist before we do anything with with the
        // block pages.

        for (int i = 0; i < newAddressPageCount; i++)
        {
            // The new address page is the user page at the user page boundary.
            long position = bouquet.getUserBoundary().getPosition();
            
            // Record the new address page.
            newAddressPages.add(position);
            
            
            // If new address page was not just created by relocating an interim
            // page, then we do not need to reserve it.
        
            // If the position is not in the free page by size, then we'll
            // attempt to reserve it from the list of free user pages.

            if (bouquet.getUserPagePool().getFreePageBySize().reserve(position))
            {
                // This user page needs to be moved.
                pagesToMove.add(position);
                
                // Remember that free user pages is a FreeSet which will
                // remove from a set of available, or add to a set of
                // positions that should not be made available. 
                bouquet.getUserPagePool().getEmptyUserPages().reserve(position);
            }
            else if (bouquet.getInterimPagePool().getFreeInterimPages().reserve(position))
            {
                // FIXME Reserve in FreeSet should now prevent a page from
                // ever returning, but the page position will be adjusted?
                pagesToMove.add(position);
            }
            else
            {
                // Was not in set of pages by size. FIXME Outgoing.
                if (!bouquet.getUserPagePool().getEmptyUserPages().reserve(position))
                {
                    // Was not in set of empty, so it is in use.
                    pagesToMove.add(position);
                }
            }

            // Move the boundary for user pages.
            bouquet.getUserBoundary().increment();
        }

        // To move a data page to make space for an address page, we simply copy
        // over the block pages that need to move, verbatim into an interim
        // block page and create a commit. The commit method will see these
        // interim block pages will as allocations, it will allocate the
        // necessary user pages and move them into a new place in the user
        // region.

        // The way that journals are written, vacuums and copies are written
        // before the operations gathered during mutation are written, so we
        // write out our address page initializations now and they will occur
        // after the blocks are copied.

        
        // If the new address page is in the set of free block pages or if it is
        // a block page we've just created the page does not have to be moved.
        
        // TODO When you expand, are you putting the user pages back? Are you
        // releasing them from ignore in the by remaining and empty tables?

        Map<Long, Long> moves = new HashMap<Long, Long>();
        for (long from : pagesToMove)
        {
            // Allocate mirrors for the user pages and place them in
            // the alloc page by size table and the allocation page set
            // so the commit method will assign a destination user page.
            long to = bouquet.getInterimPagePool().newBlankInterimPage(bouquet.getSheaf());
            journal.write(new MovePage(from, to));
            moves.put(from, to);
        }
        
        for (long position : newAddressPages)
        {
            journal.write(new CreateAddressPage(position));
        }
        
        // Run the commit.

        tryCommit();
        
        bouquet.getUserBoundary().getMoveMap().putAll(moves);
        
        interimPages = bouquet.getUserBoundary().adjust(interimPages);
        userPages = bouquet.getUserBoundary().adjust(userPages);
        
        bouquet.getInterimPagePool().getFreeInterimPages().free(interimPages);
        for (long userPage : userPages)
        {
            bouquet.getUserPagePool().returnUserPage(bouquet.getSheaf().getPage(userPage, BlockPage.class, new BlockPage()));
        }
        
        return newAddressPages;
    }

    /**
     * Create address pages, extending the address page region by moving the
     * user pages immediately follow after locking the pager to prevent
     * compaction and close.
     * 
     * @param count
     *            The number of address pages to create.
     */
    SortedSet<Long> newAddressPages(int count)
    {
        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.

        bouquet.getPageMoveLock().writeLock().lock();
        
        try
        {
            return tryNewAddressPage(count); 
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }
    }

    /**
     * Commit the mutations.
     * 
     * @param moveLatchList
     *            The per pager list of move latches associated with a move
     *            recorder specific to this commit method.
     * @param commit
     *            The state of this commit.
     */
    private void tryCommit()
    {
        for (Map.Entry<Long, Long> entry : addresses.entrySet())
        {
            journal.write(new Write(entry.getKey(), entry.getValue()));
        }
        
        journal.write(new Terminate());
        
        // Create a next pointer to point at the start of operations.
        Segment header = bouquet.getJournalHeaders().allocate();
        header.getByteBuffer().putLong(bouquet.getUserBoundary().adjust(journal.getJournalStart()));
                
        // Write and force our journal.
        dirtyPages.flush();
        header.write(bouquet.getSheaf().getFileChannel());
        try
        {
            bouquet.getSheaf().getFileChannel().force(true);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_FORCE, e);
        }
                
        // Create a journal player.
        Player player = new Player(bouquet, header, dirtyPages);
                                
        // Then do everything else.
        player.commit();

        // Unlock any addresses that were returned as free to their
        // address pages, but were locked to prevent the commit of a
        // reallocation until this commit completed.

        bouquet.getAddressLocker().unlock(player.getAddresses());
        
        Set<Long> journalPages = bouquet.getUserBoundary().adjust(journal.getJournalPages());
        bouquet.getInterimPagePool().getFreeInterimPages().free(journalPages);
    }

    /**
     * Commit the changes made to the file by this mutator. Commit will make the
     * changes made by this mutator visible to all other mutators.
     * <p>
     * After the commit is complete, the mutator may be reused.
     */
    public void commit()
    {
        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.
        bouquet.getPageMoveLock().readLock().lock();
        try
        {
            // Create a move latch list from our move latch list, with the
            // commit structure as the move recorder, so that page moves by
            // other committing mutators will be reflected in state of the
            // commit.
            tryCommit();
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }

        clear();
    }
}

/* vim: set tw=80 : */
