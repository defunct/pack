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

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;


public final class Creator
{
    private final Map<URI, Integer> mapOfStaticPageSizes;

    private int pageSize;

    private int alignment;
    
    private int addressPagePoolSize;

    private int internalJournalCount;
    
    public Creator()
    {
        this.mapOfStaticPageSizes = new TreeMap<URI, Integer>();
        this.pageSize = 8 * 1024;
        this.alignment = 64;
        this.addressPagePoolSize = 1;
        this.internalJournalCount = 64;
    }

    public void setPageSize(int pageSize)
    {
        this.pageSize = pageSize * 1024;
    }

    public void setAddressPagePoolSize(int addressPagePoolSize)
    {
        this.addressPagePoolSize = addressPagePoolSize;
    }
    
    public void setAlignment(int alignment)
    {
        this.alignment = alignment;
    }
    
    public void setInternalJournalCount(int internalJournalCount)
    {
        this.internalJournalCount = internalJournalCount;
    }

    public void addStaticPage(URI uri, int blockSize)
    {
        mapOfStaticPageSizes.put(uri, blockSize);
    }
    
    private int getStaticBlockMapSize()
    {
        int size = Pack.COUNT_SIZE;
        for (Map.Entry<URI, Integer> entry: mapOfStaticPageSizes.entrySet())
        {
            size += Pack.COUNT_SIZE + Pack.ADDRESS_SIZE;
            size += entry.getKey().toString().length() * 2;
        }
        return size;
    }

    /**
     * Create a new pack that writes to the specified file.
     */
    public Pack create(FileChannel fileChannel)
    {
        // Initialize the header.
        Header header = new Header(ByteBuffer.allocateDirect(Pack.FILE_HEADER_SIZE));
        
        header.setSignature(Pack.SIGNATURE);
        header.setShutdown(Pack.HARD_SHUTDOWN);
        header.setPageSize(pageSize);
        header.setAlignment(alignment);
        header.setInternalJournalCount(internalJournalCount);
        header.setStaticPageSize(getStaticBlockMapSize());
        header.setHeaderSize(Pack.FILE_HEADER_SIZE + header.getStaticPageSize() + header.getInternalJournalCount() * Pack.POSITION_SIZE);
        header.setAddressPagePoolSize(addressPagePoolSize);
        header.setUserBoundary(pageSize);
        header.setEndOfSheaf(0L);
        header.setFirstTemporaryNode(Long.MIN_VALUE);
        header.setFirstReferencePage(Long.MIN_VALUE);

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

        ByteBuffer journals = ByteBuffer.allocateDirect(internalJournalCount * Pack.POSITION_SIZE);

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
        Sheaf sheaf = new Sheaf(fileChannel, header.getPageSize(), header.getHeaderSize());
        
        DirtyPageSet dirtyPages = new DirtyPageSet(0);
        AddressPage addresses = sheaf.setPage(sheaf.extend(), AddressPage.class, new AddressPage(), dirtyPages);
        addresses.set(0, Long.MIN_VALUE, dirtyPages);
        dirtyPages.flush();
        
        UserBoundary userBoundary = new UserBoundary(sheaf.getPageSize(), header.getUserBoundary());
        TemporaryPool temporaryPool = new TemporaryPool(sheaf, userBoundary, header);
        
        Bouquet bouquet = new Bouquet(header, staticBlocks,
                userBoundary,
                sheaf,
                new AddressPagePool(header.getAddressPagePoolSize(), addressPages), temporaryPool);
        
        ByteBuffer statics = ByteBuffer.allocateDirect(getStaticBlockMapSize());
        
        statics.putInt(mapOfStaticPageSizes.size());
        
        if (mapOfStaticPageSizes.size() != 0)
        {
            Mutator mutator = new Pack(bouquet).mutate();
            for (Map.Entry<URI, Integer> entry: mapOfStaticPageSizes.entrySet())
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
            fileChannel.write(statics, header.getStaticPagesStart());
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
        
        return new Opener().open(fileChannel);
    }
}