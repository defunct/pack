/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.pack;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Management of a file as a reusable randomly accessible blocks of data.
 */
public class Pack
{
    /**
     * A constant value of the null address value of 0.
     */
    public final static long NULL_ADDRESS = 0L;

    public final static int ERROR_FREED_ADDRESS = 300;
    
    public final static int ERROR_FREED_STATIC_ADDRESS = 301;

    public final static int ERROR_FILE_NOT_FOUND = 400;
    
    public final static int ERROR_IO_WRITE = 401;

    public final static int ERROR_IO_READ = 402;

    public final static int ERROR_IO_SIZE = 403;

    public final static int ERROR_IO_TRUNCATE = 404;

    public final static int ERROR_IO_FORCE = 405;

    public final static int ERROR_IO_CLOSE = 406;

    public final static int ERROR_IO_STATIC_PAGES = 407;
    
    public final static int ERROR_SIGNATURE = 501;

    public final static int ERROR_SHUTDOWN = 502;

    public final static int ERROR_FILE_SIZE = 503;
    
    public final static int ERROR_HEADER_CORRUPT = 600;

    public final static int ERROR_BLOCK_PAGE_CORRUPT = 601;
    
    public final static int ERROR_CORRUPT = 602;

    final static long SIGNATURE = 0xAAAAAAAAAAAAAAAAL;
    
    final static int SOFT_SHUTDOWN = 0xAAAAAAAA;

    final static int HARD_SHUTDOWN = 0x55555555;
    
    final static int FLAG_SIZE = 2;

    final static int COUNT_SIZE = 4;

    final static int POSITION_SIZE = 8;

    final static int CHECKSUM_SIZE = 8;

    public final static int ADDRESS_SIZE = Long.SIZE / Byte.SIZE;

    final static int FILE_HEADER_SIZE = COUNT_SIZE * 7 + ADDRESS_SIZE * 5;

    public final static int BLOCK_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;

    final static int BLOCK_HEADER_SIZE = POSITION_SIZE + COUNT_SIZE;
    
    final static short MOVE_PAGE = 3;

    final static short COMMIT = 4;
    
    final static short CREATE_ADDRESS_PAGE = 5;
    
    final static short WRITE = 6;
    
    final static short FREE = 7;

    final static short NEXT_PAGE = 8;

    final static short MOVE = 9;

    final static short TERMINATE = 10;
    
    final static short TEMPORARY = 11;

    final static short CHECKPOINT = 12;

    final static int NEXT_PAGE_SIZE = FLAG_SIZE + ADDRESS_SIZE;

    final static int JOURNAL_PAGE_HEADER_SIZE = CHECKSUM_SIZE;
    
    final static int COUNT_MASK = 0xA0000000;
    
    final Bouquet bouquet;
    
    /**
     * Create a pack with the given bouquet of services.
     * 
     * @param bouquet The bouquet.
     */
    Pack(Bouquet bouquet)
    {
        this.bouquet = bouquet;
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
        return bouquet.getAlignment();
    }
    
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
                throw new PackException(Pack.ERROR_IO_CLOSE, e);
            }
        }
        finally
        {
            bouquet.getPageMoveLock().writeLock().unlock();
        }
    }
    
    
    public Mutator mutate()
    {
        return bouquet.getMutatorFactory().mutate();
    }

    /**
     * Return a map of named pages that maps a URI to the address of a static
     * page. Static pages are defined using the
     * {@link Creator#addStaticPage(URI, int)} method. They can be used to
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
        
        int userPageSize = bouquet.getUserPagePool().getSize();
        size += Pack.COUNT_SIZE + bouquet.getAddressPagePool().size() * Pack.POSITION_SIZE;
        size += Pack.COUNT_SIZE + userPageSize * Pack.POSITION_SIZE;
        
        ByteBuffer reopen = ByteBuffer.allocateDirect(size);
        
        reopen.putInt(bouquet.getAddressPagePool().size());
        for (long position: bouquet.getAddressPagePool())
        {
            reopen.putLong(position);
        }
       
        reopen.putInt(userPageSize);
        for (long position: bouquet.getUserPagePool())
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
            throw new PackException(Pack.ERROR_IO_SIZE, e);
        }
        
        // Write the variable data at the interim page positions.
        
        try
        {
            bouquet.getSheaf().getFileChannel().write(reopen, endOfSheaf);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }

        // Write the boundaries and soft shutdown flag.
        
        try
        {
            bouquet.getSheaf().getFileChannel().truncate(endOfSheaf + reopen.capacity());
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_TRUNCATE, e);
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
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */
