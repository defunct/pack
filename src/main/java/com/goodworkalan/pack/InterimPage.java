package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
  * A block page with operations specific to interim block pages such as
 * synchronization for mirror and vacuum.
 * <p>
 *
 */
final class InterimPage extends BlockPage
{
    /**
     * Return the block count as it should be written to disk.
     * <p>
     * Count is stored as it in interim pages, so this method simply returns the
     * block count. In user pages count is stored with the first bit set
     * indicating that this is a user page.
     * 
     * @return The block count value as is.
     */
    protected int getDiskCount()
    {
        return count;
    }

    /**
     * Return the count as it should be written to disk.
     * <p>
     * Count is stored as is. In user block pages the count is stored with the
     * first bit on indicating that this is a user page.
     * 
     * @param count
     *            The count read from disk.
     * @return The count unchanged.
     */
    protected int convertDiskCount(int count)
    {
        if ((count & Pack.COUNT_MASK) != 0)
        {
            throw new PackException(Pack.ERROR_CORRUPT);
        }
        return count;
    }

    /**
     * Allocate a block that is referenced by the specified address.
     * 
     * @param address
     *            The address that will reference the newly allocated block.
     * @param blockSize
     *            The full block size including the block header.
     * @param dirtyPages
     *            A dirty page map to record the block page if it changes.
     * @return True if the allocation is successful.
     */
    public void allocate(long address, int blockSize, DirtyPageSet dirtyPages)
    {
        if (blockSize < Pack.BLOCK_HEADER_SIZE)
        {
            throw new IllegalArgumentException();
        }

        synchronized (getRawPage())
        {
            ByteBuffer bytes = getBlockRange();
            boolean found = false;
            int block = 0;
            // TODO Not finding anymore. That's taken care of in commit.
            while (block != count && !found)
            {
                int size = getBlockSize(bytes);
                if (size > 0)
                {
                    block++;
                    if(getAddress(bytes) == address)
                    {
                        found = true;
                    }
                }
                bytes.position(bytes.position() + Math.abs(size));
            }
    
            if (!found)
            {
                getRawPage().invalidate(bytes.position(), blockSize);
    
                bytes.putInt(blockSize);
                bytes.putLong(address);
    
                count++;
                remaining -= blockSize;
    
                bytes.clear();
                bytes.putInt(Pack.CHECKSUM_SIZE, getDiskCount());
                getRawPage().invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
    
                dirtyPages.add(getRawPage());
            }
        }
    }

    /**
     * Copy the block at the given address to the given user page tracking dirty
     * pages in the given dirty page set.
     * 
     * @param address
     *            The address of the block to copy.
     * @param user
     *            The user page to copy to.
     * @param dirtyPages
     *            The dirty page set used to track dirty pages.
     */
    public void copy(long address, UserPage user, DirtyPageSet dirtyPages)
    {
        // When copying, we lock the interim page first. We're the only ones who
        // know about it, so it shouldn't matter though.
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                int offset = bytes.position();
                
                int blockSize = bytes.getInt();

                bytes.position(offset + Pack.BLOCK_HEADER_SIZE);
                bytes.limit(offset + blockSize);

                // User copy will lock on the user raw page.
                user.copy(address, bytes.slice(), dirtyPages);

                bytes.limit(bytes.capacity());
            }
        }
    }

    /**
     * Write the block in this interim block page with the given address to the
     * user block page resolved by dereferencing the address from its address
     * page.
     * <p>
     * The method will dereference the address from the address page, then
     * attempt to write the block to the user block page at the page position.
     * 
     * @param address
     *            The address to write.
     * @param dirtyPages
     *            The dirty page set used to track dirty pages.
     */
    public void write(long address, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                int blockSize = getBlockSize(bytes);
                bytes.limit(bytes.position() + blockSize);
                bytes.position(bytes.position() + Pack.BLOCK_HEADER_SIZE);
                

                Sheaf sheaf = getRawPage().getSheaf();
                AddressPage addresses = sheaf.getPage(address, AddressPage.class, new AddressPage());
                long lastPosition = 0L;
                for (;;)
                {
                    // FIXME Double checking does not take into account the fact
                    // that the page type at the moved address is an address
                    // page and not a user page.
                    long actual = addresses.dereference(address);
                    if (actual == 0L || actual == Long.MAX_VALUE)
                    {
                        throw new PackException(Pack.ERROR_READ_FREE_ADDRESS);
                    }
                    
                    if (actual != lastPosition)
                    {
                        UserPage user = sheaf.getPage(actual, UserPage.class, new UserPage());
                        user.waitOnMirrored();
                        synchronized (user.getRawPage())
                        {
                            if (user.write(address, bytes, dirtyPages))
                            {
                                break;
                            }
                        }
                        lastPosition = actual;
                    }
                    else
                    {
                        throw new IllegalStateException();
                    }
                }

                bytes.limit(bytes.capacity());
            }
        }
    }
    
    public void free(long address, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getBlockRange();
            int blockSize = 0;
            int block = 0;
            while (block < count)
            {
                blockSize = getBlockSize(bytes);

                assert blockSize > 0;

                if (getAddress(bytes) == address)
                {
                    break;
                }

                advance(bytes, blockSize);

                block++;
            }

            assert block != count;
            
            int to = bytes.position();
            advance(bytes, blockSize);
            int from = bytes.position();
            
            remaining += (from - to);

            block++;

            while (block < count)
            {
                blockSize = getBlockSize(bytes);
                
                assert blockSize > 0;
                
                advance(bytes, blockSize);
                
                block++;
            }
            
            int length = bytes.position() - from;
            
            for (int i = 0; i < length; i++)
            {
                bytes.put(to + i, bytes.get(from + i));
            }
            
            if (length != 0)
            {
                getRawPage().invalidate(to, length);
            }

             count--;

            getRawPage().invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
            bytes.putInt(Pack.CHECKSUM_SIZE, getDiskCount());
        }
    }
}
