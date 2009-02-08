package com.goodworkalan.pack;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.goodworkalan.sheaf.Sheaf;

/**
 * Opens pack files and performs recovery.
 */
public final class Opener
{
    private final Set<Long> setOfTemporaryBlocks;
    
    public Opener()
    {
        this.setOfTemporaryBlocks = new HashSet<Long>();
    }
    
    public Set<Long> getTemporaryBlocks()
    {
        return setOfTemporaryBlocks;
    }
    
    private boolean badAddress(Header header, long address)
    {
        return address < header.getHeaderSize() + Pack.ADDRESS_PAGE_HEADER_SIZE
            || address > header.getUserBoundary();
    }

    private Map<URI, Long> readStaticBlocks(Header header, FileChannel fileChannel) 
    {
        Map<URI, Long> staticBlocks = new TreeMap<URI, Long>();
        ByteBuffer bytes = ByteBuffer.allocateDirect(header.getStaticPageSize());
        try
        {
            fileChannel.read(bytes, header.getStaticPagesStart());
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_READ, e);
        }
        bytes.flip();
        int count = bytes.getInt();
        for (int i = 0; i < count; i++)
        {
            StringBuilder uri = new StringBuilder();
            int length = bytes.getInt();
            for (int j = 0; j < length; j++)
            {
                uri.append(bytes.getChar());
            }
            long address = bytes.getLong();
            if (badAddress(header, address))
            {
                throw new PackException(Pack.ERROR_IO_STATIC_PAGES);
            }
            try
            {
                staticBlocks.put(new URI(uri.toString()), address);
            }
            catch (URISyntaxException e)
            {
                throw new PackException(Pack.ERROR_IO_STATIC_PAGES, e);
            }
        }
        return staticBlocks;
    }    

    private Header readHeader(FileChannel fileChannel)
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(Pack.FILE_HEADER_SIZE);
        try
        {
            fileChannel.read(bytes, 0L);
        }
        catch (IOException e)
        {
           throw new PackException(Pack.ERROR_IO_READ, e);
        }
        return new Header(bytes);
    }

    public Pack open(FileChannel fileChannel)
    {
        // Read the header and obtain the basic file properties.

        Header header = readHeader(fileChannel);

        if (header.getSignature() != Pack.SIGNATURE)
        {
            throw new PackException(Pack.ERROR_SIGNATURE);
        }
        
        int shutdown = header.getShutdown();
        if (!(shutdown == Pack.HARD_SHUTDOWN || shutdown == Pack.SOFT_SHUTDOWN))
        {
            throw new PackException(Pack.ERROR_SHUTDOWN);
        }
        
        if (shutdown == Pack.HARD_SHUTDOWN)
        {
            return null;
        }

        return softOpen(fileChannel, header);
   }

    private Pack softOpen(FileChannel fileChannel, Header header)
    {
        Map<URI, Long> staticBlocks = readStaticBlocks(header, fileChannel);

        int reopenSize = 0;
        try
        {
            reopenSize = (int) (fileChannel.size() - header.getInterimBoundary());
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_SIZE, e);
        }
        
        ByteBuffer reopen = ByteBuffer.allocateDirect(reopenSize);
        try
        {
            fileChannel.read(reopen, header.getInterimBoundary());
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_READ, e);
        }
        reopen.flip();
        
        SortedSet<Long> addressPages = new TreeSet<Long>();
        
        int addressPageCount = reopen.getInt();
        for (int i = 0; i < addressPageCount; i++)
        {
            addressPages.add(reopen.getLong());
        }
        
        Map<Long, ByteBuffer> temporaryNodes = new HashMap<Long, ByteBuffer>();
        
        Sheaf pager = new Sheaf(fileChannel, header.getPageSize(), header.getHeaderSize());
        Bouquet bouquet = new Bouquet(header, staticBlocks, header.getUserBoundary(), header.getInterimBoundary(), pager, new AddressPagePool(addressPages), new TemporaryNodePool(temporaryNodes));
        
        int blockPageCount = reopen.getInt();
        for (int i = 0; i < blockPageCount; i++)
        {
            long position = reopen.getLong();
            UserPage blockPage = pager.getPage(position, UserPage.class, new UserPage());
            bouquet.getUserPagePool().returnUserPage(blockPage);
        }
        
        try
        {
            fileChannel.truncate(header.getInterimBoundary());
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_TRUNCATE, e);
        }
        
        long openBoundary = header.getInterimBoundary();
        header.setShutdown(Pack.HARD_SHUTDOWN);
        header.setInterimBoundary(0L);

        try
        {
            header.write(fileChannel, 0);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }
        
        try
        {
            fileChannel.force(true);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_FORCE, e);
        }

        Mutator mutator = bouquet.getPack().mutate();
        
        long temporaries = header.getFirstTemporaryNode();
        do
        {
            ByteBuffer node = mutator.read(temporaries);
            temporaryNodes.put(temporaries, node);
            long address = node.getLong(0);
            if (address != 0L)
            {
                setOfTemporaryBlocks.add(address);
            }
            temporaries = node.getLong(Pack.ADDRESS_SIZE);
        }
        while (temporaries != 0L);

        return new Bouquet(header, staticBlocks,
                    header.getUserBoundary(), openBoundary, pager,
                    new AddressPagePool(addressPages),
                    new TemporaryNodePool(temporaryNodes)).getPack();
    }
}