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

    private int internalJournalCount;
    
    public Creator()
    {
        this.mapOfStaticPageSizes = new TreeMap<URI, Integer>();
        this.pageSize = 8 * 1024;
        this.alignment = 64;
        this.internalJournalCount = 64;
    }

    public void setPageSize(int pageSize)
    {
        this.pageSize = pageSize * 1024;
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
        header.setUserBoundary(pageSize);
        header.setEndOfSheaf(0L);

        try
        {
            header.write(fileChannel, 0);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_WRITE, e);
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
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }

        // To create the map of static pages, we're going to allocate a
        // block from the pager. We create a local pack for this purpose.
        // This local pack will have a bogus, empty map of static pages.
        // We create a subsequent pack to return to the user.

        Map<URI, Long> staticBlocks = new HashMap<URI, Long>();

        Map<Long, ByteBuffer> temporaryNodes = new HashMap<Long, ByteBuffer>();
        
        SortedSet<Long> addressPages = new TreeSet<Long>();
        addressPages.add(0L);
        Sheaf sheaf = new Sheaf(fileChannel, header.getPageSize(), header.getHeaderSize());
        
        DirtyPageSet dirtyPages = new DirtyPageSet(0);
        sheaf.setPage(sheaf.extend(), AddressPage.class, new AddressPage(), dirtyPages, false);
        dirtyPages.flush();
        
        Bouquet bouquet = new Bouquet(header, staticBlocks,
                header.getUserBoundary(),
                sheaf,
                new AddressPagePool(addressPages),
                new TemporaryNodePool(temporaryNodes));
        
        Mutator mutator = bouquet.getMutatorFactory().mutate();
        
        header.setFirstTemporaryNode(mutator.allocate(Pack.ADDRESS_SIZE * 2));
        ByteBuffer temporaries = mutator.read(header.getFirstTemporaryNode());
        while (temporaries.remaining() != 0)
        {
            temporaries.putLong(0L);
        }
        temporaries.flip();
        mutator.write(header.getFirstTemporaryNode(), temporaries);
        
        ByteBuffer statics = ByteBuffer.allocateDirect(getStaticBlockMapSize());
        
        statics.putInt(mapOfStaticPageSizes.size());
        
        if (mapOfStaticPageSizes.size() != 0)
        {
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
        }
        
        mutator.commit();

        statics.flip();
        
        try
        {
            fileChannel.write(statics, header.getStaticPagesStart());
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }
        
        try
        {
            header.write(fileChannel, 0);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }

        new Pack(bouquet).shutdown();
        
        return new Opener().open(fileChannel);
    }
}