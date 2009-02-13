package com.goodworkalan.pack;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import com.goodworkalan.sheaf.DirtyPageSet;

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
     * A set of addresses that are temporary allocations.
     */
    private final Set<Long> temporaries;

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
        this.temporaries = new HashSet<Long>();
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
        
        bouquet.getPageMoveLock().readLock().lock();
        try
        {
            long reference = bouquet.getTemporaryPool().allocate(bouquet.getSheaf(), bouquet.getHeader(), bouquet.getUserBoundary(), bouquet.getInterimPagePool(), dirtyPages);
            journal.write(new Temporary(address, reference));
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }
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
        addressPage = bouquet.getAddressPagePool().getAddressPage(bouquet, lastAddressPage);
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

        // Each of the allocations of temporary blocks, blocks that are returned
        // by the opener when the file is reopened, needs to be rolled back. 
        for (long temporary : temporaries)
        {
            bouquet.getTemporaryPool().free(temporary, bouquet.getSheaf(), bouquet.getUserBoundary(), dirtyPages);
        }
        
        // Write any dirty pages.
        dirtyPages.flush();
        
        // Put the interim pages we used back into the set of free interim
        // pages.
        for (long position : bouquet.getUserBoundary().adjust(bouquet.getSheaf(), interims))
        {
            bouquet.getSheaf().free(position);
            bouquet.getInterimPagePool().free(position);
        }
        
        for (long position : bouquet.getUserBoundary().adjust(bouquet.getSheaf(), journal.getJournalPages()))
        {
            bouquet.getSheaf().free(position);
            bouquet.getInterimPagePool().free(position);
        }
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
     * Commit the mutations.
     */
    private void tryCommit()
    {
        for (Map.Entry<Long, Long> entry : addresses.entrySet())
        {
            journal.write(new Write(entry.getKey(), entry.getValue()));
        }
        
        journal.write(new Commit());
        
        // The vacuum journal could get lost, but so could any uncommitted
        // transaction. Block pages outstanding, no user address reference.
        journal.write(new Terminate());

        // Create a journal player.
        new Player(bouquet, journal, dirtyPages).commit();
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
