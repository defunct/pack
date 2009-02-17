/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.pack;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.testng.annotations.Test;

import com.goodworkalan.sheaf.DirtyRegionMap;

public class PackTestCase
{
    private File newFile()
    {
        try
        {
            File file = File.createTempFile("momento", ".mto");
            file.deleteOnExit();
            return file;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    private FileChannel newFileChannel(File file)
    {
        FileChannel fileChannel;
        try
        {
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
        }
        catch (FileNotFoundException e)
        {
            throw new PackException(PackException.ERROR_FILE_NOT_FOUND, e);
        }
        return fileChannel;
    }

    private FileChannel newFileChannel()
    {
        try
        {
            File file = File.createTempFile("momento", ".mto");
            file.deleteOnExit();
            // Open the file.
            FileChannel fileChannel;
            try
            {
                fileChannel = new RandomAccessFile(file, "rw").getChannel();
            }
            catch (FileNotFoundException e)
            {
                throw new PackException(PackException.ERROR_FILE_NOT_FOUND, e);
            }
            return fileChannel;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test public void create()
    {
        new Creator().create(newFileChannel()).close();
    }

    @Test(expectedExceptions=java.lang.IllegalStateException.class) public void regionalLowerRange()
    {
        final ByteBuffer expected = ByteBuffer.allocateDirect(64);

        DirtyRegionMap regional = new DirtyRegionMap(0L)
        {
            @Override
            public ByteBuffer getByteBuffer()
            {
                return expected;
            }
        };
        regional.invalidate(-1, 10);
    }

    @Test(expectedExceptions=java.lang.IllegalStateException.class) public void regionalUpperRange()
    {
        final ByteBuffer expected = ByteBuffer.allocateDirect(64);

        DirtyRegionMap regional = new DirtyRegionMap(0L)
        {
            @Override
            public ByteBuffer getByteBuffer()
            {
                return expected;
            }
        };
        regional.invalidate(0, 65);
    }

    @Test public void header()
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(Pack.FILE_HEADER_SIZE);
        Header header = new Header(bytes);
        header.setSignature(0);
        assertEquals(header.getSignature(), 0);
    }

    @Test public void reopen()
    {
        File file = newFile();
        new Creator().create(newFileChannel(file)).close();
        new Opener().open(newFileChannel(file)).close();
        new Opener().open(newFileChannel(file)).close();
    }

    @Test public void commit()
    {
        File file = newFile();
        FileChannel fileChannel = newFileChannel(file);
        Pack pack = new Creator().create(fileChannel);
        Mutator mutator = pack.mutate();
        mutator.commit();
        pack.close();
        fileChannel = newFileChannel(file);
        new Opener().open(fileChannel).close();
    }

    @Test public void allocate()
    {
        File file = newFile();
        FileChannel fileChannel = newFileChannel(file);
        Pack pack = new Creator().create(fileChannel);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        mutator.commit();
        pack.close();
        new Opener().open(newFileChannel(file)).close();
    }
    
    @Test public void badSignature() throws IOException
    {
        File file = newFile();
        FileChannel fileChannel = newFileChannel(file);
        
        new Creator().create(fileChannel).close();
        
        ByteBuffer bytes = ByteBuffer.allocateDirect(1);
        bytes.put((byte) 0);
        bytes.flip();

        fileChannel = newFileChannel(file);
        fileChannel.write(bytes, 0L);
        fileChannel.close();

        fileChannel = newFileChannel(file);
        try
        {
            new Opener().open(fileChannel);
        }
        catch (PackException e)
        {
            assertEquals(PackException.ERROR_SIGNATURE, e.getCode());
            return;
        }

        fail("Expected exception not thrown.");
    }

    private ByteBuffer get64bytes()
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        return bytes;
    }
    
    @Test public void write()
    {
        FileChannel file = newFileChannel();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        mutator.write(address, bytes);

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }
        
        mutator.commit();
        
        mutator = pack.mutate();

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        
        pack.close();
    }
    
    @Test public void rewrite()
    {
        FileChannel file = newFileChannel();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        mutator = pack.mutate();
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        
        mutator.write(address, bytes);
        
        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        mutator = pack.mutate();

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();

        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        
        pack.close();
    }
    
    @Test public void collect()
    {
        FileChannel file = newFileChannel();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        System.gc();
        System.gc();
        System.gc();
        
        mutator = pack.mutate();
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        
        mutator.write(address, bytes);
        
        System.gc();
        System.gc();
        System.gc();

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        
        System.gc();
        System.gc();
        System.gc();

        mutator = pack.mutate();

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();

        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        
        pack.close();
    }
    
    @Test public void rewriteMany()
    {
        FileChannel file = newFileChannel();
        Pack pack = new Creator().create(file);

        rewrite(pack, 12);
                
        pack.close();
    }

    public void rewrite(Pack pack, int count)
    {
        Mutator mutator = pack.mutate();
        long[] addresses = new long[count];
        for (int i = 0; i < count; i++)
        {
            addresses[i] = mutator.allocate(64);
        }
        mutator.commit();
        
        mutator = pack.mutate();
        for (int i = 0; i < count; i++)
        {
            ByteBuffer bytes = ByteBuffer.allocateDirect(64);
            for (int j = 0; j < 64; j++)
            {
                bytes.put((byte) j);
            }
            bytes.flip();
            
            mutator.write(addresses[i], bytes);
        }

        for (int i = 0; i < count; i++)
        {
            assertBuffer(mutator, addresses[i], 64);
        }

        mutator.commit();
        mutator = pack.mutate();
        for (int i = 0; i < count; i++)
        {
            assertBuffer(mutator, addresses[i], 64);
        }

        mutator.commit();
    }

    private void assertBuffer(Mutator mutator, long address, int count)
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < count; i++)
        {
            assertEquals((byte) i, bytes.get());
        }
    }

    @Test public void free()
    {
        FileChannel fileChannel = newFileChannel();
        Pack pack = new Creator().create(fileChannel);
        
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        mutator = pack.mutate();
        mutator.free(address);
        mutator.commit();

        boolean thrown = false;
        mutator = pack.mutate();
        try
        {
            mutator.read(address, ByteBuffer.allocateDirect(64));
        }
        catch (PackException e)
        {
            thrown = true;
            assertEquals(PackException.ERROR_FREED_ADDRESS, e.getCode());
        }
        assertTrue(thrown);
        mutator.commit();

        pack.close();
    }

    @Test public void freeAndClose()
    {
        File file = newFile();
        FileChannel fileChannel = newFileChannel(file);
        Pack pack = new Creator().create(fileChannel);
        
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        pack.close();
        fileChannel = newFileChannel(file);
        pack = new Opener().open(fileChannel);
        
        mutator = pack.mutate();
        mutator.free(address);
        mutator.commit();

        boolean thrown = false;
        mutator = pack.mutate();
        try
        {
            mutator.read(address, ByteBuffer.allocateDirect(64));
        }
        catch (PackException e)
        {
            thrown = true;
            assertEquals(PackException.ERROR_FREED_ADDRESS, e.getCode());
        }
        assertTrue(thrown);
        mutator.commit();

        pack.close();
    }
    
    @Test public void freeWithContext()
    {
        FileChannel file = newFileChannel();
        Pack pack = new Creator().create(file);
        
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        rewrite(pack, 3);

        mutator = pack.mutate();
        mutator.free(address);
        mutator.commit();

        boolean thrown = false;
        mutator = pack.mutate();
        try
        {
            mutator.read(address, ByteBuffer.allocateDirect(64));
        }
        catch (PackException e)
        {
            thrown = true;
            assertEquals(PackException.ERROR_FREED_ADDRESS, e.getCode());
        }
        assertTrue(thrown);
        mutator.commit();

        pack.close();
    }

    @Test public void mulipleJournalPages()
    {
        FileChannel file = newFileChannel();
        Pack pack = new Creator().create(file);

        rewrite(pack, 1);
        
        Mutator mutator = pack.mutate();
        for (int i = 0; i < 800; i++)
        {
            mutator.allocate(64);
        }
        mutator.commit();
        
        pack.close();
    }

    @Test public void moveUserPageForAddress()
    {
        FileChannel file = newFileChannel();
        Pack pack = new Creator().create(file);

        rewrite(pack, 1);
        
        Mutator mutator = pack.mutate();
        for (int i = 0; i < 1000; i++)
        {
            mutator.allocate(64);
        }
        mutator.commit();
        
        pack.close();
    }
    
    @Test(expectedExceptions=java.lang.UnsupportedOperationException.class)
    public void bySizeTableIteratorRemove()
    {
        List<SortedSet<Long>> listOfListsOfSizes = new ArrayList<SortedSet<Long>>();
        listOfListsOfSizes.add(new TreeSet<Long>());
        Iterator<Long> iterator = new ByRemainingTableIterator(listOfListsOfSizes);
        iterator.remove();
    }
    
    @Test(expectedExceptions=java.lang.ArrayIndexOutOfBoundsException.class)
    public void bySizeTableIteratorOutOfBounds()
    {
        List<SortedSet<Long>> listOfListsOfSizes = new ArrayList<SortedSet<Long>>();
        listOfListsOfSizes.add(new TreeSet<Long>());
        Iterator<Long> iterator = new ByRemainingTableIterator(listOfListsOfSizes);
        iterator.next();
    }
    
    @Test public void staticPages()
    {
        Creator creator = new Creator();
        creator.addStaticPage(URI.create("http://one.com/"), 64);
        creator.addStaticPage(URI.create("http://two.com/"), 64);
        FileChannel fileChannel = newFileChannel();
        Pack pack = creator.create(fileChannel);
        Mutator mutator = pack.mutate();
        mutator.write(mutator.getPack().getStaticBlocks().get(URI.create("http://one.com/")), get64bytes());
        mutator.commit();
    }
    
    public void moveInterimPageForAddress()
    {
        FileChannel file = newFileChannel();
        Pack pack = new Creator().create(file);

        rewrite(pack, 8000);
                
        pack.close();
    }

    public void softRecover()
    {
        Creator newPack = new Creator();
        /* This needs to be rewritten when pack takes FileChannel instead.
        newPack.setDisk(new Disk()
        {
            int count = 0;

            @Override
            public FileChannel truncate(FileChannel fileChannel, long size) throws IOException
            {
                if (count++ == 2)
                {
                    fileChannel.close();
                    throw new IOException();
                }
                return fileChannel.truncate(size);
            }
        }); */
        FileChannel file = newFileChannel();
        Pack pack = newPack.create(file);
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        mutator.write(address, bytes);
        mutator.commit();
        boolean thrown = false;
        try
        {
            pack.close();
        }
        catch (PackException e)
        {
            thrown = true;
        }
        assertTrue(thrown);
        Opener opener = new Opener();
        pack = opener.open(file);
        mutator = pack.mutate();
        assertBuffer(mutator, address, 64);
        mutator.commit();
        pack.close();
    }
    
    @Test public void rollback() 
    {
        File file = newFile();
        FileChannel fileChannel = newFileChannel(file);
        Pack pack = new Creator().create(fileChannel);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        mutator.commit();
        mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.rollback();
        pack.close();
        fileChannel = newFileChannel(file);
        new Opener().open(fileChannel).close();
        fileChannel = newFileChannel(file);
        pack = new Opener().open(fileChannel);
        mutator = pack.mutate();
        assertEquals(address, mutator.allocate(64));
        mutator.rollback();
        pack.close();
    }
    
    @Test public void vacuum()
    {
        File file = newFile();
        FileChannel fileChannel = newFileChannel(file);
        Pack pack = new Creator().create(fileChannel);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        mutator.commit();
        pack.vacuum();
        pack.close();
        new Opener().open(newFileChannel(file)).close();
    }

    @Test public void vacuum2()
    {
        File file = newFile();
        FileChannel fileChannel = newFileChannel(file);
        Pack pack = new Creator().create(fileChannel);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        long address = mutator.allocate(64);
        mutator.commit();
        rewrite(pack, 4);
        pack.close();
        fileChannel = newFileChannel(file);
        pack = new Opener().open(fileChannel);
        mutator = pack.mutate();
        mutator.free(address);
        mutator.commit();
        mutator = pack.mutate();
        assertEquals(address, mutator.allocate(64));
        mutator.commit();
        pack.close();
    }
    
    @Test public void vacuumAtOffset()
    {
        File file = newFile();
        FileChannel fileChannel = newFileChannel(file);
        Pack pack = new Creator().create(fileChannel);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        long address1 = mutator.allocate(64);
        mutator.allocate(64);
        long address2 = mutator.allocate(64);
        mutator.commit();
        rewrite(pack, 4);
        pack.close();
        fileChannel = newFileChannel(file); 
        pack = new Opener().open(fileChannel);
        mutator = pack.mutate();
        mutator.free(address1);
        mutator.free(address2);
        mutator.commit();
        mutator = pack.mutate();
        assertEquals(address1, mutator.allocate(64));
        mutator.commit();
        pack.close();
    }

    @Test public void temporary()
    {
        File file = newFile();
        FileChannel fileChannel = newFileChannel(file);
        Pack pack = new Creator().create(fileChannel);
        Mutator mutator = pack.mutate();
        mutator.temporary(64);
        mutator.commit();
        pack.close();
        
        Opener opener = new Opener();
        fileChannel = newFileChannel(file);
        pack = opener.open(fileChannel);
        mutator = pack.mutate();
        for (long address : opener.getTemporaryBlocks())
        {
            mutator.free(address);
        }
        mutator.commit();
        pack.close();
        
        fileChannel = newFileChannel(file);
        opener = new Opener();
        pack = opener.open(fileChannel);
        assertEquals(0, opener.getTemporaryBlocks().size());
    }
    
    
    @Test public void unallocate()
    {
        FileChannel file = newFileChannel();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        mutator.allocate(13);
        long allocate = mutator.allocate(9);
        long write1 = mutator.allocate(72);
        long write2 = mutator.allocate(82);
        mutator.free(allocate);
        mutator.commit();
        mutator.write(write1, get64bytes());
        mutator.write(write2, get64bytes());
        mutator.free(write2);
        mutator.commit();
        mutator.allocate(21);
        allocate = mutator.allocate(37);
        mutator.free(allocate);
        mutator.allocate(73);
        mutator.commit();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */