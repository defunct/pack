package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.zip.Adler32;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * A block page with operations specific to user block pages such as
 * synchronization for mirror and vacuum.
 * <p>
 * The
 * {@link #mirror(Adler32, boolean, InterimPagePool, Sheaf, InterimPage, DirtyPageSet)
 * mirror} method copies the contents of this user block page to an interim
 * block page for move and for vacuum.
 * <p>
 * When moving the user page, all blocks are copied to an interim page. At
 * commit the mirrored blocks are copied to the destination user page.
 * <p>
 * When vacuuming the user page, all blocks after the first block that is
 * preceded by an freed block are copied to an interim page. At commit
 * the mirrored blocks are copied into place, overwriting the freed blocks.
 * <p>
 * The {@link #copy(long, ByteBuffer, DirtyPageSet) copy} method copies a single
 * block from a mirror of the user page.
 * 
 * @author Alan Gutierrez
 */
final class UserPage extends BlockPage
{
    /**
     * True if the page is in the midst of a vacuum and should not be written
     * to.
     */
    private boolean mirrored;

    /**
     * Return the count as it should be written to disk.
     * <p>
     * Count is stored with the first bit set indicating that this is a user
     * page.
     * <p>
     * @return The integer count value with the first bit set. 
     */
    protected int getDiskCount()
    {
        return count | Pack.COUNT_MASK;
    }

    /**
     * Convert the count written to disk into the actual count value.
     * <p>
     * Count is stored with the first bit set indicating that this is a user
     * page.
     * 
     * @param count The count read from disk.
     * @return The count with the first bit off.
     */
    protected int convertDiskCount(int count)
    {
        if ((count & Pack.COUNT_MASK) == 0)
        {
            throw new PackException(Pack.ERROR_CORRUPT);
        }
        return count & ~Pack.COUNT_MASK;
    }

    /**
     * If the page is mirrored for vacuum, wait for the vacuum to complete.
     * <p>
     * This is called before write and before free. If we were to write
     * a block or free a block that was mirrored, then then the vacuum
     * copies the mirror into place, the mirror would be overwritten.
     */
    public synchronized void waitOnMirrored()
    {
        while (mirrored)
        {
            try
            {
                wait();
            }
            catch (InterruptedException e)
            {
            }
        }
    }

    /**
     * Mirror the user block page to the given interim block page.
     * <p>
     * The vacuum parameter indicates that mirroring should begin at the first
     * allocated block beyond the first free block. If there are no free blocks
     * to vacuum, then <code>mirror</code> returns null. If the interim
     * parameter is null, then the <code>sheaf</code> will be used to allocate
     * an <code>InterimPage</code> if vacuum is necessary.
     * <p>
     * Upon mirroring the user page, any subsequent write or free operation will
     * block until the {@link #unmirror()} method is called. Until
     * <code>unmirror</code> is called a hard reference to each mirrored user
     * page must be held by the caller in order to ensure that all mutators
     * reference the same user page. The hard reference must be released after
     * <code>unmirror</code> is called.
     * <p>
     * FIXME When mirror is called and returns null, what happens?
     * 
     * @param adler32
     *            Used to record the checksum of the block pages.
     * @param vacuum
     *            If true, mirror only if there are free blocks between
     *            allocated blocks, if false, mirror from the first block.
     * @param interimPagePool
     *            The interim page pool used to allocate a block page if the
     *            given interim page is null and a vacuum is actually necessary.
     * @param sheaf
     *            The sheaf used to allocate a block page if the given interim
     *            page is null and a vacuum is actually necessary.
     * @param interim
     *            The interim page to which this page is mirrored, or null for
     *            only if needed allocation.
     * @param dirtyPages
     *            The set of dirty pages.
     * @return A mirror object with information on mirrored interim page given
     *         or the mirrored page allocated, or null if no interim page was
     *         given nor allocated.
     */
    public synchronized Mirror mirror(Adler32 adler32, boolean vacuum, InterimPagePool interimPagePool, Sheaf sheaf, InterimPage interim, DirtyPageSet dirtyPages)
    {
        // Should not mirror if already mirrored.
        if (mirrored)
        {
            throw new IllegalStateException();
        }

        // If always vacuum, start at offset 0.
        int offset = vacuum ? -1 : 0;
        
        Mirror mirror = null;
        
        // Synchronize on the raw page.
        synchronized (getRawPage())
        {
            // Get the byte bufer with the position at the first block.
            ByteBuffer bytes = getBlockRange();

            // Iterate over the blocks, advancing if the block count if the
            // block has not been freed.
            int block = 0;
            while (block != count)
            {
                // If the block size is negative, then the block has been freed.
                // The absolute block size is the size of the block.
                int size = getBlockSize(bytes);
                if (size < 0)
                {
                    // If the offset has not been set already, set the offset to
                    // begin at this block count.
                    if (offset == -1)
                    {
                        offset = block;
                    }
                    advance(bytes, size);
                }
                else
                {
                    // Increment the block count.
                    block++;

                    // If we have an offset, we mirror the block, otherwise we
                    // advance to the next block. 
                    if (offset == -1)
                    {
                        advance(bytes, size);
                    }
                    else
                    {
                        // Allocate the interim page if it has not given or
                        // already allocated.
                        if (interim == null)
                        {
                            interim = interimPagePool.newInterimPage(sheaf, InterimPage.class, new InterimPage(), dirtyPages);
                        }

                        // Allocate a block in the interim page.
                        int blockSize = bytes.getInt();
                        long address = bytes.getLong();
                        
                        interim.allocate(address, blockSize, dirtyPages);

                        // Write the user block to the interim block.
                        int userSize = blockSize - Pack.BLOCK_HEADER_SIZE;

                        bytes.limit(bytes.position() + userSize);
                        interim.write(address, bytes, dirtyPages);
                        bytes.limit(bytes.capacity());
                    }
                } 
            }
            
            // If we were given or have allocated an interim block, then we have
            // in fact mirrored the user page.
            mirrored = mirror != null;
            
            // Return a mirror structure including a checksum of the block
            // contents.
            if (mirrored)
            {
                mirror = new Mirror(interim, offset, getChecksum(adler32));
            }
        }
        
        return mirror;
    }

    /**
     * Notify all mutators waiting to write or free a block on this mirrored
     * page that a vacuum has completed.
     */
    public synchronized void unmirror()
    {
        if (!mirrored)
        {
            throw new IllegalStateException();
        }
        mirrored = false;
        notifyAll();
    }

    /**
     * Vacuum the user page, truncating the block count the given offset, and
     * copying the blocks from the given mirrored interim block page.
     * <p>
     * The given adler32 checksum engine is used to generate a checksum which is
     * compared against the given checksum to assert that the vacuum preserved
     * the contents of the page.
     * 
     * @param adler32
     *            A checksum engine.
     * @param checksum
     *            The checksum recorded when the user page was mirrored.
     * @param mirrored
     *            The interim block page containing the user page blocks.
     * @param offset
     *            The offset of the first block contained in the interim block
     *            page.
     * @param dirtyPages
     *            The dirty page set used to track dirty pages.
     */
    public void vacuum(Adler32 adler32, long checksum, InterimPage mirrored, int offset, DirtyPageSet dirtyPages)
    {
        // When copying, we lock the interim page first. We're the only ones who
        // know about it, so it shouldn't matter though.
        synchronized (mirrored.getRawPage())
        {
            // Synchronize on the user raw page.
            synchronized (getRawPage())
            {
                // The offset must be less than the count.
                if (!(offset < count))
                {
                    throw new IllegalStateException();
                }
                // There interim page must have the correct amount of blocks.
                if (count - offset != mirrored.getCount())
                {
                    throw new IllegalStateException();
                }
                // Truncate the user block page.
                count = offset;
                // Copy each block from the interim block page.
                for (long address : mirrored.getAddresses())
                {
                    // This will synchronize on the interim then user pages a
                    // second time.
                    mirrored.copy(address, this, dirtyPages);
                }
                // Assert the checksum.
                if (checksum != getChecksum(adler32))
                {
                    throw new IllegalStateException();
                }
            }
        }
    }

    /**
     * Copy a block into this user block page from a interim block page where
     * the blocks have been mirrored.
     * <p>
     * This method will check to see if the block alreay exists. If it already
     * exists, it writes the block like the
     * {@link BlockPage#write(long, ByteBuffer, DirtyPageSet) write} method of
     * {@link BlockPage}. Unlike {@link BlockPage}, if the block does not exist,
     * it will append the block.
     * <p>
     * This method also updates the address page, so that the address in the
     * address page references the file position of this user page.
     * 
     * @param address
     *            The address of the block.
     * @param block
     *            The contents of the block.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    public void copy(long address, ByteBuffer block, DirtyPageSet dirtyPages)
    {
        // Synchronize on the raw page.
        synchronized (getRawPage())
        {
            // Dereference the file position of the address.
            Sheaf sheaf = getRawPage().getSheaf();
            AddressPage addresses = sheaf.getPage(address, AddressPage.class, new AddressPage());
            long position = addresses.dereference(address);

            // If the file position of the address does not agree with us, set
            // the file position to that of the current raw page.
            if (position != getRawPage().getPosition())
            {
                // Position must not be null.
                if (position == 0L)
                {
                    throw new IllegalStateException();
                }

                // Set the address to the new file position. In the case of a
                // vacuum, the file position will not change. In the case of a
                // page move, the file position will change, but we don't need
                // to free the block froim the current user block page, since it
                // will simply be drained an reassigned as an address page.
                if (position != getRawPage().getPosition())
                {
                    addresses.set(address, getRawPage().getPosition(), dirtyPages);
                }
            }
            
            // If the block exists in the page, overwrite the block. Otherwise,
            // append the block to the end of the user page.
            
            // FIXME Does not work. It will skip freed blocks, recreating the
            // bad mojo.
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                int size = bytes.getInt();
                
                if (size != block.remaining() + Pack.BLOCK_HEADER_SIZE)
                {
                    throw new IllegalStateException();
                }
                
                if (bytes.getLong() != address)
                {
                    throw new IllegalStateException();
                }
                
                getRawPage().invalidate(bytes.position(), block.remaining());
                bytes.put(block);
            }
            else
            {
                if (block.remaining() + Pack.BLOCK_HEADER_SIZE > bytes.remaining())
                {
                    throw new IllegalStateException();
                }
                
                getRawPage().invalidate(bytes.position(), block.remaining() + Pack.BLOCK_HEADER_SIZE);
                
                remaining -= block.remaining() + Pack.BLOCK_HEADER_SIZE;
                
                bytes.putInt(block.remaining() + Pack.BLOCK_HEADER_SIZE);
                bytes.putLong(address);
                bytes.put(block);
                
                count++;
                
                getRawPage().invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
                bytes.putInt(Pack.CHECKSUM_SIZE, getDiskCount());
            }

            dirtyPages.add(getRawPage());
        }
    }

    /**
     * Free a user block page.
     * <p>
     * The page is altered so that the block is skipped when the list of blocks
     * is iterated. If the block is the last block, then the bytes are 
     * immediately available for reallocation. If teh block is followed by one
     * or more blocks, the bytes are not available for reallocation until the
     * user page is vacuumed (or until all the blocks after this block are
     * freed, but that is not at all predictable.)
     * 
     * @param address
     *            The address of the block to free.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    public void free(long address, DirtyPageSet dirtyPages)
    {
        // Synchronize on the raw page.
        synchronized (getRawPage())
        {
            // If the block is found, negate the block size, indicating that the
            // absolute value of the block size should be skipped when iterating
            // the blocks.
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                int offset = bytes.position();

                int size = bytes.getInt();
                if (size > 0)
                {
                    size = -size;
                }

                getRawPage().invalidate(offset, Pack.COUNT_SIZE);
                bytes.putInt(offset, size);
                
                count--;
                getRawPage().invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
                bytes.putInt(Pack.CHECKSUM_SIZE, getDiskCount());

                dirtyPages.add(getRawPage());
            }
        }
    }

    /**
     * Generate a checksum of the block contents of the page. This will generate
     * a checksum of the blocks and block headers, excluding the free blocks and
     * unallocated bytes.
     * <p>
     * This must be called in a synchronized block.
     * 
     * @param adler32
     *            The checksum to use.
     * 
     * @return A checksum.
     */
    public long getChecksum(Adler32 adler32)
    {
        adler32.reset();
    
        ByteBuffer bytes = getRawPage().getByteBuffer();
        bytes.clear();
        bytes.position(Pack.CHECKSUM_SIZE);
    
        for (int i = 0; i < Pack.COUNT_SIZE; i++)
        {
            adler32.update(bytes.get());
        }
    
        int block = 0;
        while (block < count)
        {
            int size = getBlockSize(bytes);
            if (size > 0)
            {
                for (int i = 0; i < size; i++)
                {
                    adler32.update(bytes.get());
                }
                block++;
            }
            else
            {
                bytes.position(bytes.position() + -size);
            }
        }
    
        return adler32.getValue();
    }
}
