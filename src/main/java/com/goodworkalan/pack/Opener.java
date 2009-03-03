package com.goodworkalan.pack;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.goodworkalan.sheaf.Sheaf;

/**
 * Opens a <code>Pack</code> file.
 */
public final class Opener
{
    /** A set of user defined temporary blocks to store intermeiate states. */ 
    private final Set<Long> temporaryBlocks;
    
    /** The pack opened by this opener. */
    private Pack pack;
   
    /**
     * Create a new file opener.
     */
    public Opener()
    {
        this.temporaryBlocks = new HashSet<Long>();
    }

    /**
     * Return the addresses of blocks in the file marked as temporary by the
     * {@link Mutator#setTemporary(long)}.
     * 
     * @return The set of temporary blocks.
     */
    public Set<Long> getTemporaryBlocks()
    {
        return temporaryBlocks;
    }
    
    /**
     * Read the map of static blocks URIs to block address from the given file
     * channel using the housekeeping information in the given header.
     * 
     * @param header
     *            The file header.
     * @param fileChannel
     *            The file channel.
     * @return A map of static block URIs to block addresses.
     * @throws URISyntaxException If a static URI used to name a block address is malformed.
     * @throws IOException If an I/O error occurs while reading the static blocks.
     */
    private Map<URI, Long> readStaticBlocks(Header header, FileChannel fileChannel) throws URISyntaxException, IOException 
    {
        Map<URI, Long> staticBlocks = new TreeMap<URI, Long>();
        ByteBuffer bytes = ByteBuffer.allocateDirect(header.getStaticBlockCount());
        fileChannel.read(bytes, header.getStaticBlockMapStart());
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
            staticBlocks.put(new URI(uri.toString()), address);
        }
        return staticBlocks;
    }

    /**
     * Read the file header from the given file channel.
     * 
     * @param fileChannel
     *            The file channel.
     * @return The file header.
     * @throws PackException
     *             If an I/O error occurs.
     */
    private Header readHeader(FileChannel fileChannel)
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(Pack.FILE_HEADER_SIZE);
        try
        {
            fileChannel.read(bytes, 0L);
        }
        catch (IOException e)
        {
           throw new PackException(PackException.ERROR_IO_READ, e);
        }
        return new Header(bytes);
    }

    /**
     * Opens a file structure that is stored in the given file channel. Returns
     * false if the file structure was subject to a hard shutdown. If the file
     * was subject to a hard shutdown, the {@link #getPack()} method will return
     * a null value.
     * <p>
     * When subject to a hard shutdown, client programmers should take action to
     * backup the file and to run a recovery using a {@link Medic}.
     * <p>
     * The opener will only test to see if the file is a {@link Pack} file and
     * check the shutdown flag. A false return value could mean that the file
     * was subject to a hard shutdown or could mean that the file is not a
     * {@link Pack} file at all. A client programmer can learn more about the
     * state of the underlying file using {@link Medic}.
     * 
     * @param fileChannel
     *            The file channel.
     * @return True if the file opened successfully, false if the file is not a
     *         <code>Pack</code> or it was subject to a hard shutdown.
     * @throws IllegalStateException
     *             If the opener has already opened a file channel.
     * 
     */
    public boolean open(FileChannel fileChannel)
    {
        if (pack != null)
        {
            throw new IllegalStateException();
        }

        try
        {
            // Read the header and obtain the basic file properties.
            Header header = readHeader(fileChannel);
    
            if (header.getSignature() != Pack.SIGNATURE)
            {
                return false;
            }
            
            int shutdown = header.getShutdown();
            if (shutdown != Pack.SOFT_SHUTDOWN)
            {
                return false;
            }
    
            Map<URI, Long> staticBlocks = readStaticBlocks(header, fileChannel);
    
            int reopenSize = 0;
            reopenSize = (int) (fileChannel.size() - header.getAddressLookupPagePool());
            
            ByteBuffer reopen = ByteBuffer.allocateDirect(reopenSize);
            fileChannel.read(reopen, header.getAddressLookupPagePool());
            reopen.flip();
            
            SortedSet<Long> addressPages = new TreeSet<Long>();
            
            int addressPageCount = reopen.getInt();
            for (int i = 0; i < addressPageCount; i++)
            {
                addressPages.add(reopen.getLong());
            }
            
            fileChannel.truncate(header.getAddressLookupPagePool());
            
            Sheaf sheaf = new Sheaf(fileChannel, header.getPageSize(), header.getHeaderSize());
            AddressBoundary userBoundary = new AddressBoundary(sheaf, header.getUserBoundary());
            InterimPagePool interimPagePool = new InterimPagePool(sheaf);
            TemporaryPool temporaryPool = new TemporaryPool(sheaf, header, userBoundary, interimPagePool);
            temporaryBlocks.addAll(temporaryPool.toMap().keySet());
            
            Set<Long> freedBlockPages = new HashSet<Long>();
            int blockPageCount = reopen.getInt();
            for (int i = 0; i < blockPageCount; i++)
            {
                long position = reopen.getLong();
                BlockPage user = sheaf.getPage(position, BlockPage.class, new BlockPage());
                freedBlockPages.add(user.getRawPage().getPosition());
            }
    
            header.setShutdown(Pack.HARD_SHUTDOWN);
            header.setAddressLookupPagePool(0L);
    
            header.write(fileChannel, 0);
            
            fileChannel.force(true);
    
            Bouquet bouquet = new Bouquet(header, staticBlocks, userBoundary, sheaf,
                        new AddressPagePool(header.getAddressPagePoolSize(), addressPages), 
                        interimPagePool, temporaryPool);
            bouquet.getUserPagePool().vacuum(bouquet);
            pack = bouquet.getPack();
            
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    /**
     * Get the pack opened by this opener.
     * 
     * @return The pack opened by this opener.
     * @throws IllegalStateException
     *             If the opener has not successfully opened a file.
     */
    public Pack getPack()
    {
        if (pack == null)
        {
            throw new IllegalStateException();
        }
        return pack;
    }
}