/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.pack;

import java.io.IOException;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.goodworkalan.pack.vacuum.Vacuum;
import com.goodworkalan.sheaf.DirtyPageSet;

/**
 * Management of a file as a reusable randomly accessible blocks of data.
 * <p>
 * Although this file structure is concurrent, isolated, and durable, it is not
 * atomic. A commit will update addresses so that isolated blocks become the
 * common blocks, however other mutators will see this change the moment it
 * happens. If two blocks allocated by a mutator, as the mutator commits, one
 * block will become visible to the user before the other block becomes visible.
 * <p>
 * Pack also supports exposing raw pages that are managed by the user. Raw pages
 * allocated via the file structure are dereferenced by an address, so they can
 * move to accommodate new address pages. The file structure reserves the first
 * bit of the page for housekeeping information. First bit, that is, not byte,
 * so you can use the first byte to store primitive Java types so long as the
 * value is not negative.
 */
public class Pack
{
    /**  The null address value, zero. */
    public final static long NULL_ADDRESS = 0L;

    /** A value written at start of the file to identify the file. */
    final static long SIGNATURE = 0xAAAAAAAAAAAAAAAAL;
    
    /** The flag for a soft shutdown. */
    final static int SOFT_SHUTDOWN = 0xAAAAAAAA;

    /** The flag indicating a hard shutdown. */
    final static int HARD_SHUTDOWN = 0x55555555;
    
    /** Size in bytes of a primitive integer. */
    final static int INT_SIZE = (Integer.SIZE / Byte.SIZE);
    
    /** Size in bytes of a primitive long. */
    final static int LONG_SIZE = (Long.SIZE / Byte.SIZE);

    /** Size in bytes of a primitive short. */
    final static int SHORT_SIZE = (Short.SIZE / Byte.SIZE);

    /** Size of the file header. */
    final static int FILE_HEADER_SIZE = INT_SIZE * 7 + LONG_SIZE * 5;

    /** Size of a block page header, the page size. */
    public final static int BLOCK_PAGE_HEADER_SIZE = INT_SIZE;

    /** Size of a block header, back reference address and block size. */
    final static int BLOCK_HEADER_SIZE = LONG_SIZE + INT_SIZE;
    
    /** The bouquet of services. */
    final Bouquet bouquet;
    
    /**
     * Create a pack with the given bouquet of services.
     * 
     * @param bouquet The bouquet of services.
     */
    Pack(Bouquet bouquet)
    {
        this.bouquet = bouquet;
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
            do
            {
                BlockPage user = bouquet.getUserBoundary().dereference(bouquet.getSheaf(), address);
                read = user.read(address, destination);
            }
            while (read == null);
            
            return read;
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }
    }

    /**
     * Read the block at the given address into the a buffer and return the
     * buffer. This method will allocate a byte buffer of the block size.
     * <p>
     * The address is only valid after the <code>commit</code> method of the
     * <code>Mutator</code> that allocated the address is called.
     * 
     * @param address
     *            The block address.
     * @return A buffer containing the contents of the block.
     */
    public ByteBuffer read(long address)
    {
        return tryRead(address, null);
    }

    /**
     * Read the block referenced by the given address into the given destination
     * buffer. If the block size is greater than the bytes remaining in the
     * destination buffer, size of the addressed block, no bytes are transferred
     * and a <code>BufferOverflowException</code> is thrown.
     * <p>
     * The address is only valid after the <code>commit</code> method of the
     * <code>Mutator</code> that allocated the address is called.
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
        tryRead(address, destination);
    }
    
    /**
     * Return the maximum size of a block allocation for this pack.
     * 
     * @return The maximum block size.
     */
    public int getMaximumBlockSize()
    {
        return bouquet.getSheaf().getPageSize() - Pack.BLOCK_PAGE_HEADER_SIZE - Pack.BLOCK_HEADER_SIZE;
    }
    
    /**
     * Get the size of all underlying pages managed by this pager.
     * 
     * @return The page size.
     */
    public int getPageSize()
    {
        return bouquet.getSheaf().getPageSize();
    }

    /**
     * Return the alignment to which all block allocations are rounded.
     * 
     * @return The block alignment.
     */
    public int getAlignment()
    {
        return bouquet.getHeader().getAlignment();
    }
    
    /**
     * Set the strategy for optimizing the size of the file on disk.
     * 
     * @param vacuum
     *            The strategy for optimizing the size of the file on disk.
     */
    public void setVacuum(Vacuum vacuum)
    {
        synchronized (bouquet.getVacuumMutex())
        {
            bouquet.getUserPagePool().setVacuum(vacuum);
        }
    }

    /**
     * Optimize the size of the pack on disk by combining block pages and
     * compacting block pages with freed blocks.
     * <p>
     * The actual vacuum is only performed if the {@link Vacuum} strategy
     * assigned by the {@link Creator} determines that there is something to
     * gain from a vacuum.
     * <p>
     * Vacuum can either be called after each mutator commit or it can be
     * called periodically from a worker thread.
     * 
     * @see Vacuum
     * @see Creator
     */
    public void vacuum()
    {
        synchronized (bouquet.getVacuumMutex())
        {
            bouquet.getPageMoveLock().readLock().lock();
            try
            {
                bouquet.getUserPagePool().vacuum(bouquet);
            }
            finally
            {
                bouquet.getPageMoveLock().readLock().unlock();
            }
        }
    }

    /**
     * Soft close of the pack will wait until all mutators commit or rollback
     * and then compact the pack before closing the file.
     */
    public void close()
    {
        // Grab the exclusive compact lock, which will wait for any concurrent
        // commits to complete.
    
        bouquet.getPageMoveLock().writeLock().lock();
    
        try
        {
            shutdown();
            try
            {
                bouquet.getSheaf().getFileChannel().close();
            }
            catch(IOException e)
            {
                throw new PackException(PackException.ERROR_IO_CLOSE, e);
            }
        }
        finally
        {
            bouquet.getPageMoveLock().writeLock().unlock();
        }
    }
    
    /**
     * Create an isolated mutator that can concurrently modify the pack. A mutator
     * is the only way to 
     * 
     * The
     * changed made by the mutator will not be made permanent until the
     * {@link Mutator#commit() commit} method of the mutator is called. They
     * will be discarded
     * 
     * @return
     */
    public Mutator mutate()
    {
        DirtyPageSet dirtyPages = new DirtyPageSet();
        Journal journal = new Journal(bouquet.getSheaf(), bouquet.getInterimPagePool(), dirtyPages);
        return new Mutator(bouquet, journal, dirtyPages);
    }

    /**
     * Return a map of named pages that maps a URI to the address of a static
     * page. Static pages are defined using the
     * {@link Creator#addStaticBlock(URI, int)} method. They can be used to
     * specify blocks that contain housekeeping information in application
     * programs.
     * 
     * @return The map of named static pages.
     */
    public Map<URI, Long> getStaticBlocks()
    {
        return bouquet.getStaticBlocks();
    }

    /**
     * This method would keep a page bitset. It would go through every address
     * page and mark a bit for each user page. It would ask the vacuum for an
     * accounting of all the free pages. Hmm... OK. Lock down allocations and
     * commits, so addresses don't change, read through all the addresses, then
     * you, resume, but you turn off page reuse, by zeroing out the empty pages,
     * then you can leisurely survey the pages in between.
     * <p>
     * Ah, you could zero out the reuse structures, know that new pages come off
     * the wilderness, so your bit set survey ignores pages off the top. Then
     * walk through the address pages, knowing that if you encounter a reference
     * then it is accounted for, at the end, you will have a set of missing
     * pages, you can give those to free list, it can read through them at each
     * vacuum and reintroduce them.
     * <p>
     * Alternatively, I could keep a journal page pool. Track references to the
     * journal pages. Oh, I'm doing that already. Hmm, not quite.
     * <p>
     * What about quitting by writing an instruction, to a next pointer that
     * says, these journal pages, don't run them, just ask them which pages were
     * involved. Which next operations to the start, but only reports on what
     * pages were jostled. This means that headers are not a fixed number, but
     * that the journals lie around until vacuumed, which is what we're doing
     * now anyway. Use an AddressPage to keep track of them.
     * <p>
     * Then use an address page for temporaries, be done with it.
     * <p>
     * Now, you only lose pages when you crash before commit. Still need to
     * survey, reap, super-vacuum if you want all your pages back.
     * <p>
     * Create the actions Force, FreeHeader, and the like.
     * <p>
     * TODO Even more magic.
     */
    public void survey()
    {
    }
    
    /**
     * Close the pager writing out the region boundaries and the soft shutdown
     * flag in the header and variable pager state to a region at the end of the
     * file. The variable data includes address pages with addresses remaining,
     * empty user pages, and user pages with space remaining. The variable data
     * is positioned at the location indicated by the user to interim boundary.
     * <p>
     * Close will wait for any concurrent commits to complete.
     */
    void shutdown()
    {
        // Write the set of address pages, the set of empty user pages and
        // the set of pages with space remaining to a byte buffer.

        int size = 0;
        
        size += INT_SIZE + bouquet.getAddressPagePool().size() * Pack.LONG_SIZE;
        
        ByteBuffer reopen = ByteBuffer.allocateDirect(size);
        
        reopen.putInt(bouquet.getAddressPagePool().size());
        for (long position: bouquet.getAddressPagePool())
        {
            reopen.putLong(position);
        }
       
        reopen.flip();

        long endOfSheaf;
        try
        {
            endOfSheaf = bouquet.getSheaf().getFileChannel().size();
        }
        catch(IOException e)
        {
            throw new PackException(PackException.ERROR_IO_SIZE, e);
        }
        
        // Write the variable data at the interim page positions.
        
        try
        {
            bouquet.getSheaf().getFileChannel().write(reopen, endOfSheaf);
        }
        catch (IOException e)
        {
            throw new PackException(PackException.ERROR_IO_WRITE, e);
        }

        // Write the boundaries and soft shutdown flag.
        
        try
        {
            bouquet.getSheaf().getFileChannel().truncate(endOfSheaf + reopen.capacity());
        }
        catch (IOException e)
        {
            throw new PackException(PackException.ERROR_IO_TRUNCATE, e);
        }
        
        bouquet.getHeader().setUserBoundary(bouquet.getUserBoundary().getPosition());
        bouquet.getHeader().setEndOfSheaf(endOfSheaf);

        bouquet.getHeader().setShutdown(Pack.SOFT_SHUTDOWN);
        try
        {
            bouquet.getHeader().write(bouquet.getSheaf().getFileChannel(), 0);
        }
        catch (IOException e)
        {
            throw new PackException(PackException.ERROR_IO_WRITE, e);
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */
