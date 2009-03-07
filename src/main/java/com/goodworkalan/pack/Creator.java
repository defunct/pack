package com.goodworkalan.pack;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.goodworkalan.region.Header;
import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Used to create a <code>Pack</code> and specify the properties of the
 * <code>Pack</code> that once created are immutable.
 * 
 * @author Alan Gutierrez
 */
public final class Creator
{
    /** A map of named pages to page sizes. */
    private final Map<URI, Integer> staticPages;

    /** The page size. */
    private int pageSize;

    /** The block alignment. */
    private int alignment;
    
    /**
     * The minimum number of address pages with addresses available for
     * allocation.
     */
    private int addressPagePoolSize;

    /** The number of journal headers in the file. */
    private int internalJournalCount;
    
    /**
     * Create a constructor for a <code>Pack</code>. The default values for
     * each property are documented in the setter for the poperty.
     */
    public Creator()
    {
        this.staticPages = new TreeMap<URI, Integer>();
        this.pageSize = 8 * 1024;
        this.alignment = 64;
        this.addressPagePoolSize = 1;
        this.internalJournalCount = 64;
    }
    
    /**
     * Set the page size size in kilobytes. The minimum page size is 1k.
     * 
     * @param pageSize The page size.
     */
    public void setPageSize(int pageSize)
    {
        this.pageSize = pageSize * 1024;
    }

    /**
     * Set the block alignment.
     * <p>
     * The block alignment is used to group pages in by bytes available for
     * allocation in lookup tables for free blocks. The default byte alignment
     * is 64 bytes. The minimum byte alignment is 32 bytes.
     * 
     * @param alignment The block alignment.
     */
    public void setAlignment(int alignment)
    {
        this.alignment = alignment;
    }

    /**
     * Add a static block identified by a URI that cannot be deallocated.
     * <p>
     * Static blocks can be used by client programmers to create headers and
     * housekeeping records.
     * 
     * @param uri
     *            The URI of the static block.
     * @param blockSize
     *            The size of the static block.
     */
    public void addStaticBlock(URI uri, int blockSize)
    {
        staticPages.put(uri, blockSize);
    }

    /**
     * Set the number of journal headers in the file. The number of journal
     * headers determines the maximum number of concurrent commit operations.
     * The default is 64.
     * 
     * @param internalJournalCount
     *            The number of journals in the file.
     */
    public void setInternalJournalCount(int internalJournalCount)
    {
        this.internalJournalCount = internalJournalCount;
    }

    /**
     * Set the minimum number of address pages with addresses available to keep
     * in the address page pool. When the number of address pages with addresses
     * available for allocation drops below this minimum a new address page is
     * created from the user pages.
     * 
     * @param addressPagePoolSize
     *            The minimum number of address pages with addresses available
     *            to keep in the address page pool
     */
    public void setAddressPagePoolSize(int addressPagePoolSize)
    {
        this.addressPagePoolSize = addressPagePoolSize;
    }

    /**
     * Get the size in bytes necessary to store the static block map.
     * 
     * @return The size necessary to store the static block map.
     */
    private int getStaticBlockMapSize()
    {
        int size = Pack.INT_SIZE;
        for (Map.Entry<URI, Integer> entry: staticPages.entrySet())
        {
            size += Pack.INT_SIZE + Pack.LONG_SIZE;
            size += entry.getKey().toString().length() * 2;
        }
        return size;
    }

    /**
     * Create a new pack that writes to the specified file channel.
     * 
     * @return fileChannel The file channel.
     */
    public Pack create(FileChannel fileChannel)
    {
        // Initialize the header.
        Header<Integer> header = Housekeeping.newHeader();
        
        header.get(Housekeeping.SIGNATURE).getByteBuffer().putLong(0, Pack.SIGNATURE);
        header.get(Housekeeping.SHUTDOWN).getByteBuffer().putInt(0, Pack.HARD_SHUTDOWN);
        header.get(Housekeeping.PAGE_SIZE).getByteBuffer().putInt(0, pageSize);
        header.get(Housekeeping.ALIGNMENT).getByteBuffer().putInt(0, alignment);
        header.get(Housekeeping.JOURNAL_COUNT).getByteBuffer().putInt(0, internalJournalCount);
        header.get(Housekeeping.STATIC_BLOCK_COUNT).getByteBuffer().putInt(0, getStaticBlockMapSize());
        header.get(Housekeeping.HEADER_SIZE).getByteBuffer().putInt(0, Pack.FILE_HEADER_SIZE + getStaticBlockMapSize() + internalJournalCount * Pack.LONG_SIZE);
        header.get(Housekeeping.ADDRESS_PAGE_POOL_SIZE).getByteBuffer().putInt(0, addressPagePoolSize);
        header.get(Housekeeping.ADDRESS_BOUNDARY).getByteBuffer().putLong(0, pageSize);
        header.get(Housekeeping.ADDRESS_LOOKUP_PAGE_POOL).getByteBuffer().putLong(0, 0L);
        header.get(Housekeeping.FIRST_TEMPORARY_RESOURCE_PAGE).getByteBuffer().putLong(0, Long.MIN_VALUE);
        header.get(Housekeeping.BY_REMAINING_TABLE).getByteBuffer().putLong(0, Long.MIN_VALUE);

        try
        {
            header.write(fileChannel, 0);
        }
        catch (IOException e)
        {
            throw new PackException(PackException.ERROR_IO_WRITE, e);
        }

        // Create a buffer of journal file positions. Initialize each page
        // position to 0. Write the journal headers to file.

        ByteBuffer journals = ByteBuffer.allocateDirect(internalJournalCount * Pack.LONG_SIZE);

        for (int i = 0; i < internalJournalCount; i++)
        {
            journals.putLong(0L);
        }

        journals.flip();

        try
        {
            fileChannel.write(journals, Pack.FILE_HEADER_SIZE);
        }
        catch (IOException e)
        {
            throw new PackException(PackException.ERROR_IO_WRITE, e);
        }

        // To create the map of static pages, we're going to allocate a
        // block from the pager. We create a local pack for this purpose.
        // This local pack will have a bogus, empty map of static pages.
        // We create a subsequent pack to return to the user.

        Map<URI, Long> staticBlocks = new HashMap<URI, Long>();

        SortedSet<Long> addressPages = new TreeSet<Long>();
        addressPages.add(0L);
        Sheaf sheaf = new Sheaf(fileChannel, pageSize, header.get(Housekeeping.HEADER_SIZE).getByteBuffer().getInt(0));
        
        DirtyPageSet dirtyPages = new DirtyPageSet();
        AddressPage addresses = sheaf.setPage(sheaf.extend(), AddressPage.class, new AddressPage(), dirtyPages);
        addresses.set(0, Long.MIN_VALUE, dirtyPages);
        dirtyPages.flush();
        
        AddressBoundary userBoundary = new AddressBoundary(sheaf, sheaf.getPageSize());
        InterimPagePool interimPagePool = new InterimPagePool(sheaf);
        TemporaryPool temporaryPool = new TemporaryPool(sheaf, header, userBoundary, interimPagePool);
        Bouquet bouquet = new Bouquet(header, staticBlocks,
                userBoundary,
                sheaf,
                new AddressPagePool(addressPagePoolSize, addressPages), interimPagePool,
              temporaryPool);
        
        ByteBuffer statics = ByteBuffer.allocateDirect(getStaticBlockMapSize());
        
        statics.putInt(staticPages.size());
        
        if (staticPages.size() != 0)
        {
            Mutator mutator = new Pack(bouquet).mutate();
            for (Map.Entry<URI, Integer> entry: staticPages.entrySet())
            {
                String uri = entry.getKey().toString();
                int size = entry.getValue();
                long address = mutator.allocate(size);
                statics.putInt(uri.length());
                for (int i = 0; i < uri.length(); i++)
                {
                    statics.putChar(uri.charAt(i));
                }
                statics.putLong(address);
            }
            mutator.commit();
        }

        statics.flip();
        
        try
        {
            fileChannel.write(statics, Housekeeping.getStaticBlockMapStart(header, internalJournalCount));
        }
        catch (IOException e)
        {
            throw new PackException(PackException.ERROR_IO_WRITE, e);
        }
        
        try
        {
            header.write(fileChannel, 0);
        }
        catch (IOException e)
        {
            throw new PackException(PackException.ERROR_IO_WRITE, e);
        }

        new Pack(bouquet).shutdown();
        
        Opener opener = new Opener();
        opener.open(fileChannel);
        return opener.getPack();
    }
}