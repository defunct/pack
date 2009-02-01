package com.goodworkalan.pack;

import java.io.File;
import java.io.FileNotFoundException;
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
import com.goodworkalan.sheaf.Disk;
import com.goodworkalan.sheaf.Sheaf;


public final class Creator
{
    private final Map<URI, Integer> mapOfStaticPageSizes;

    private int pageSize;

    private int alignment;

    private int internalJournalCount;
    
    private Disk disk;

    public Creator()
    {
        this.mapOfStaticPageSizes = new TreeMap<URI, Integer>();
        this.pageSize = 8 * 1024;
        this.alignment = 64;
        this.internalJournalCount = 64;
        this.disk = new Disk();
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
    
    /**
     * Implements a wrapper around 
     * @param disk
     */
    void setDisk(Disk disk)
    {
        this.disk = disk;
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
    public Pack create(File file)
    {
        // Open the file.
        FileChannel fileChannel;
        try
        {
            fileChannel = disk.open(file);
        }
        catch (FileNotFoundException e)
        {
            throw new PackException(Pack.ERROR_FILE_NOT_FOUND, e);
        }
        
        Offsets offsets = new Offsets(pageSize, internalJournalCount, getStaticBlockMapSize());
        ByteBuffer fullSize = ByteBuffer.allocateDirect((int) (offsets.getFirstAddressPage() + pageSize));
        try
        {
            disk.write(fileChannel, fullSize, 0L);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }
        
        // Initialize the header.
        
        Header header = new Header(ByteBuffer.allocateDirect(Pack.FILE_HEADER_SIZE));
        
        header.setSignature(Pack.SIGNATURE);
        header.setShutdown(Pack.HARD_SHUTDOWN);
        header.setPageSize(pageSize);
        header.setAlignment(alignment);
        header.setInternalJournalCount(internalJournalCount);
        header.setStaticPageSize(getStaticBlockMapSize());
        header.setFirstAddressPageStart(Pack.FILE_HEADER_SIZE + header.getStaticPageSize() + header.getInternalJournalCount() * Pack.POSITION_SIZE);
        header.setDataBoundary(0L);
        header.setOpenBoundary(0L);

        try
        {
            header.write(disk, fileChannel, 0);
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
        addressPages.add(offsets.getFirstAddressPage());
        Sheaf pager = new Sheaf(fileChannel, disk, header.getPageSize(), header.getFirstAddressPageStart());
        Bouquet bouquet = new Bouquet(file, header,
                staticBlocks, header.getDataBoundary(), header.getDataBoundary(),
                pager,
                new AddressPagePool(addressPages),
                new TemporaryServer(temporaryNodes));
        
        long user = bouquet.getInterimBoundary().getPosition();
        bouquet.getInterimBoundary().increment();
        
        DirtyPageSet dirtyPages = new DirtyPageSet(bouquet.getSheaf(), 0);
        bouquet.getSheaf().setPage(user, UserPage.class, new UserPage(), dirtyPages, false);
        UserPage blocks = bouquet.getSheaf().getPage(user, UserPage.class, new UserPage());
        blocks.getRawPage().invalidate(0, pageSize);
        dirtyPages.flush();
        
        bouquet.getUserPagePool().returnUserPage(blocks);
        
        Mutator mutator = bouquet.getMutatorFactory().mutate();
        
        header.setTemporaries(mutator.allocate(Pack.ADDRESS_SIZE * 2));
        ByteBuffer temporaries = mutator.read(header.getTemporaries());
        while (temporaries.remaining() != 0)
        {
            temporaries.putLong(0L);
        }
        temporaries.flip();
        mutator.write(header.getTemporaries(), temporaries);
        
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
            disk.write(fileChannel, statics, header.getStaticPagesStart());
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }
        
        try
        {
            header.write(disk, fileChannel, 0);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }

        new Pack(bouquet).close();
        
        Opener opener = new Opener();
        
        opener.setDisk(disk);
        
        return opener.open(file);
    }
}