package com.goodworkalan.pack;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;
import com.goodworkalan.sheaf.Sheaf;

/**
 * <p>
 * A relocatable page that contains a list of data blocks.
 * <h3>Duplicate Soft References</h3>
 * <p>
 * The data page is the only page that can take advantage of the duplication
 * soft references in the raw page class. The raw page holds a soft reference to
 * the byte buffer. It is itself soft referenced by the map of pages by position
 * in the pager.
 * <p>
 * All pages write their changes out to the byte buffer. We hold onto dirty raw
 * pages in a dirty page map. Once the raw page is written to disk we let go of
 * the hard reference to the raw page in the raw page map. It can be collected.
 * <p>
 * The data page also contains a lock that keeps another mutator from writing to
 * it when it is being vacuumed. The lock is based on the <code>wait</code> and
 * <code>notify</code> methods of the data page object. The byte buffer may be
 * flushed to disk, but a data page waiting to be vacuumed still needs to be
 * held in memory because of the lock.
 * <h4>Two-Stage Vacuum</h4>
 * <p>
 * Vacuum must work in two stages. The page is mirrored. The blocks that are
 * preceded by one or more freed blocks are copied into interim pages. Then
 * during journal play back, the compacting is performed by copying the mirrored
 * blocks into place over the freed blocks.
 * <p>
 * Once a page is mirrored, no other mutator can write to that page, since that
 * would put it out of sync with the mirroring of the page. If we were to mirror
 * a page and then another mutator updated a block in the page, if the blocks is
 * preceded by one or more freed blocks, then that block would be reverted when
 * we compact the page from the mirror.
 * <p>
 * Initially, you thought that a strategy was have the writing mutator also
 * update the mirror. This caused a lot of confusion, since now the journal was
 * changing after the switch to play back. How does one mutator write to another
 * mutator's journal? Which mutator commits that change? This raised so many
 * questions, I can't remember them all.
 * <p>
 * The mirrored property is checked before an mutator writes or frees a block.
 * If it is true, indicating that the page is mirrored but not compacted, then
 * the operation will block until the compacting makes the vacuum complete.
 * <p>
 * Vacuums occur before all other play back operations. During play back after a
 * hard shutdown, we run the vacuums before all other operations. We run the
 * vacuums of each journal, then we run the remainder of each journal.
 * <h4>Deadlock</h4>
 * <p>
 * Every once and a while, you forget and worry about deadlock. You're afraid
 * that one thread holding on a mirrored data page will attempt to write to a
 * mirrored data page of anther thread while that thread is trying to write a
 * mirrored data page held the this thread. This cannot happen, of course,
 * because vacuums happen before write or free operations.
 * <p>
 * You cannot deadlock by mirroring, because only one mutator at a time will
 * ever vacuum a data page, because only one mutator at a time can use a data
 * page for block allocation.
 */
class BlockPage extends Page
{
    /** The count of blocks in this page. */
    private int count;

    /** The count of bytes remaining for block allocation. */
    private int remaining;

    /**
     * Construct an uninitialized block page that is then initialized by calling
     * the {@link #create create} or {@link #load load} methods. The default
     * constructor creates an empty address page that must be initialized before
     * use.
     * <p>
     * All of the page classes have default constructors. This constructor is
     * called by clients of the <code>Sheaf</code> when requesting pages or
     * creating new pages.
     * <p>
     * An uninitialized page of the expected Java class of page is given to the
     * <code>Sheaf</code>. If the page does not exist, the empty, default
     * constructed page is used, if not is ignored and garbage collected. This
     * is a variation on the prototype object construction pattern.
     * 
     * @see com.goodworkalan.sheaf.Sheaf#getPage(long, Class, Page)
     * @see com.goodworkalan.sheaf.Sheaf#setPage(long, Class, Page,
     *      DirtyPageSet, boolean)
     */
    public BlockPage()
    {
    }

    /**
     * Create a block page under the given raw page.
     * 
     * @param rawPage
     *            The raw page.
     * @param dirtyPages
     *            The collection of dirty pages.
     * @see Sheaf#setPage(long, Class, Page, DirtyPageSet, boolean)
     */
    public void create(DirtyPageSet dirtyPages)
    {
        this.count = 0;
        this.remaining = getRawPage().getSheaf().getPageSize() - Pack.BLOCK_PAGE_HEADER_SIZE;

        ByteBuffer bytes = getRawPage().getByteBuffer();

        bytes.clear();

        getRawPage().invalidate(0, Pack.BLOCK_PAGE_HEADER_SIZE);
        bytes.putLong(0L);
        bytes.putInt(getDiskBlockCount());

        dirtyPages.add(getRawPage());
    }

    // TODO Document.
    private int calcRemaining()
    {
        int remaining = getRawPage().getSheaf().getPageSize() - Pack.BLOCK_PAGE_HEADER_SIZE;
        ByteBuffer bytes = getBlockRange();
        for (int i = 0; i < count; i++)
        {
            int size = Math.abs(getBlockSize(bytes));
            remaining -= size;
            advance(bytes, size);
        }
        return remaining;
    }

    /**
     * Load the block page from the underlying raw page.
     */
    public void load()
    {
        ByteBuffer bytes = getRawPage().getByteBuffer();

        bytes.clear();
        bytes.getLong();

        count = - bytes.getInt();
        remaining = calcRemaining();
    }

    /**
     * Get the count of blocks in this page.
     * 
     * @return The count of blocks in this page.
     */
    public int getBlockCount()
    {
        synchronized (getRawPage())
        {
            return count;
        }
    }
    
    private int getDiskBlockCount()
    {
        return -count;
    }

    /**
     * Get the count of bytes remaining for block allocation.
     * 
     * @return The count of bytes remaining for block allocation.
     */
    public int getRemaining()
    {
        synchronized (getRawPage())
        {
            return remaining;
        }
    }
    
    /**
     * Get the full block size including the block header of the block at the
     * position of the given byte buffer of the underlying block page.
     * 
     * @param bytes
     *            The content of this block page with the position set to
     *            reference a block in in the block page.
     * @return The full block size of the block.
     */
    protected int getBlockSize(ByteBuffer bytes)
    {
        int blockSize = bytes.getInt(bytes.position());
        assert blockSize != 0;
        assert Math.abs(blockSize) <= bytes.remaining();
        return blockSize;
    }

    /**
     * Return the back-referenced address of the block at the current position
     * of the given byte buffer without adjusting the byte buffer position.
     * 
     * @param bytes
     *            The page byte buffer.
     * @return The back-referenced address.
     */
    protected long getAddress(ByteBuffer bytes)
    {
        return Math.abs(bytes.getLong(bytes.position() + Pack.INT_SIZE));
    }

    /**
     * Return true if the block is part of an allocation that was too big to fit
     * in the maximum block size and is followed by one or more subsequent
     * blocks that continue the allocation.
     * <p>
     * Continued allocations are indicated by storing a negated value for the
     * back-reference address.
     * 
     * @param address
     *            The block address.
     * @return True if the block is part of a continued allocation.
     */
    public Boolean isContinued(long address)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                return bytes.getLong(bytes.position() + Pack.INT_SIZE) < 0L;
            }
        }
        
        return null;
    }

    /**
     * Set to true flag that indicates that block is part of an allocation that
     * was too big to fit in the maximum block size and is followed by one or
     * more subsequent blocks that continue the allocation. Return true if the
     * block for the given address exists in this block page, false if it does
     * not. If the block does not exist, it is likely that the block has been
     * moved by a vacuum and the address need to be dereferenced again.
     * <p>
     * Continued allocations are indicated by storing a negated value for the
     * back-reference address.
     * 
     * @param address
     *            The block address.
     * @return True if the block for the given address was found in this block
     *         page, false if it was not.
     */
    public boolean setContinued(long address)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                bytes.putLong(bytes.position() + Pack.INT_SIZE, -getAddress(bytes));
                return true;
            }
        }
        
        return false;
    }

    /**
     * Set offset the position of the given byte buffer by the absolute value of
     * the given block size.
     * 
     * @param bytes
     *            The block page byte buffer.
     * @param blockSize
     *            A block size read from the block page.
     */
    protected void advance(ByteBuffer bytes, int blockSize)
    {
        bytes.position(bytes.position() + Math.abs(blockSize));
    }

    /**
     * Return the given byte buffer containing a block page with the position
     * set to the range of bytes that contain blocks.
     * 
     * @param bytes
     *            The block page byte buffer.
     * @return The byte buffer limited to the block range.
     */
    private ByteBuffer getBlockRange(ByteBuffer bytes)
    {
        bytes.position(Pack.BLOCK_PAGE_HEADER_SIZE);
        return bytes;
    }

    /**
     * Return the underlying byte buffer with the position set to the position
     * of the first free or allocated block in the block page.
     * <p>
     * This method must be called while synchronized on the underlying raw page.
     * 
     * @return The underlying byte buffer with the position at the first block.
     */
    protected ByteBuffer getBlockRange()
    {
        return getBlockRange(getRawPage().getByteBuffer());
    }

    /**
     * Advance to the block associated with the address in this page. If found
     * the position of the byte buffer will be at the start of the full block
     * including the block header. If not found the block is after the last
     * valid block.
     * 
     * @param bytes
     *            The byte buffer of this block page.
     * @param address
     *            The address to seek.
     * @return True if the address is found, false if not found.
     */
    protected boolean seek(ByteBuffer bytes, long address)
    {
        bytes = getBlockRange(bytes);
        for (int i = 0; i < count; i++)
        {
            int size = getBlockSize(bytes);
            if (size > 0 && getAddress(bytes) == address)
            {
                return true;
            }
            advance(bytes, size);
        }
        return false;
    }

    /**
     * Truncate the block page, adjusting the block count to end the list of
     * blocks after the block with the given address.
     * <p>
     * This method is called during journal playback of a vacuum move to reset
     * the block page to its state when the vacuum move was recorded in the
     * journal. If there is a hard shutdown while blocks are appended to the
     * block page, the truncating the block page before appending moved blocks
     * will ensure that the when the journal is replayed in recovery, it does
     * not append a block twice. The truncate and append will overwrite any
     * blocks that were corrupted by a hard shutdown in middle of a block write.
     * 
     * @param address
     *            The block address.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    public void truncate(long address, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            dirtyPages.add(getRawPage());
            count = 0;
            if (address != 0L)
            {
                ByteBuffer bytes = getBlockRange();
                for (;;)
                {
                    count++;
                    int size = getBlockSize(bytes);
                    if (size > 0 && getAddress(bytes) == address)
                    {
                        break;
                    }
                    advance(bytes, size);
                }
            }
            getRawPage().invalidate(Pack.LONG_SIZE, Pack.INT_SIZE);
            getRawPage().getByteBuffer().putInt(Pack.LONG_SIZE, getDiskBlockCount());
        }
    }

    /**
     * Return true if there are no gaps left by free blocks in the list of
     * allocated blocks.
     * <p>
     * A page is discontinuous when a freed block is followed by one or more
     * allocated blocks.
     * <p>
     * Not all block frees cause a block page to become discontinuous. When a
     * block or string of blocks is freed from the end of the list of blocks, no
     * gaps are created and the page is continuous. If a page is discontinuous,
     * the allocated blocks following the first freed block are freed, the page
     * becomes continuous.
     * 
     * @return True if there are no freed blocks followed by allocated blocks.
     */
    // TODO Document.
    public boolean purge(DirtyPageSet dirtyPages)
    {
        ByteBuffer bytes = getBlockRange();
        boolean continuous = true;
        int freed = 0;
        int free = 0;
        for (int i = 0; i < count; i++)
        {
            int size = getBlockSize(bytes);
            if (size < 0)
            {
                free++;
                freed += Math.abs(size);
            }
            else if (free != 0)
            {
                continuous = false;
                free = 0;
                freed = 0;
            }
            advance(bytes, size);
        }
        if (free != 0)
        {
            count -= free;
            remaining += freed;
            dirtyPages.add(getRawPage());
            getRawPage().getByteBuffer().putInt(0, getDiskBlockCount());
        }
        return continuous;
    }

    /**
     * Get the full block size including the block header of the block in this
     * block page at the given address. Returns zero if the block is not in
     * this block page.
     * 
     * @param address
     *            The address of the block.
     * @return The full block size of the block.
     */
    public int getBlockSize(long address)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                return getBlockSize(bytes);
            }
        }
        return 0;
    }

    /**
     * Return a list of the addresses recorded in the address back-references
     * position of each block in the page.
     * 
     * @return A list of the addresses of the blocks in this page.
     */
    public List<Long> getAddresses()
    {
        List<Long> listOfAddresses = new ArrayList<Long>(getBlockCount());
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getBlockRange();
            for (int i = 0; i < count; i++)
            {
                int size = getBlockSize(bytes);
                if (size > 0)
                {
                    listOfAddresses.add(getAddress(bytes));
                }
                advance(bytes, size);
            }
        }
        return listOfAddresses;
    }

    /**
     * Get the address of the last block in the list of allocated blocks for use
     * in truncating failed copies during journal playback. This method is
     * called to obtain the last block in the list of blocks when writing a
     * journal entry for a block page move.
     * <p>
     * During playback, the journal will truncate the block page, ignoring any
     * blocks that occur after the recorded last address. Then it will append
     * new blocks to the block page. This truncate is for the case where a hard
     * shutdown occurs while appending blocks during a vacuum move. The truncate
     * ensures that we do not append a block twice, that we do a complete do
     * over, resetting the state of the block page, overwritting any blocks that
     * might have been corrupted by a hard shutdown in middle of a write.
     * 
     * @return The address of the last block in the list of allocated blocks.
     */
    public long getLastAddress()
    {
        long last = 0L;
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getBlockRange();
            for (int i = 0; i < count; i++)
            {
                int size = getBlockSize(bytes);
                if (size > 0)
                {
                    last = getAddress(bytes);
                }
                advance(bytes, size);
            }
        }
        return last;
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
            
            // FIXME Assert that we go to the end of the page.
            seek(bytes, address);

            getRawPage().invalidate(bytes.position(), blockSize);

            bytes.putInt(blockSize);
            bytes.putLong(address);

            count++;
            remaining -= blockSize;

            bytes.clear();
            bytes.putInt(Pack.LONG_SIZE, getDiskBlockCount());
            getRawPage().invalidate(Pack.LONG_SIZE, Pack.INT_SIZE);

            dirtyPages.add(getRawPage());
        }
    }

    /**
     * Write the given data to the page at the block referenced by the given
     * block address if the block exists in the this block page. If the block
     * does not exist in this block page, this method does not alter the
     * underlying page.
     * <p>
     * The given dirty page set is used to record the underlying raw page as
     * dirty, if the method writes the block.
     * <p>
     * Returns true if the block exists in this page and is written, false if
     * the block does not exist.
     * 
     * @param address
     *            The block address.
     * @param data
     *            The data to write.
     * @param dirtyPages
     *            The set of dirty pages.
     * @return True if the block exists in this page and is written, false if
     *         the block does not exist.
     * @throws BufferOverflowException
     *             If there is insufficient space in the block for the remaining
     *             bytes in the source buffer.
     */
    public boolean write(long address, ByteBuffer data, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                int offset = bytes.position();
                int size = bytes.getInt();
                if (bytes.getLong() != address)
                {
                    throw new PackException(PackException.ERROR_BLOCK_PAGE_CORRUPT);
                }
                bytes.limit(offset + size);
                if (bytes.remaining() < data.remaining())
                {
                    throw new BufferOverflowException();
                }
                getRawPage().invalidate(bytes.position(), bytes.remaining());
                bytes.put(data);
                bytes.limit(bytes.capacity());
                dirtyPages.add(getRawPage());
                return true;
            }
            return false;
        }
    }

    /**
     * Find the block referenced by the given address in this block page and
     * read the contents into the given destination buffer. If the given
     * destination buffer is null, this method will allocate a byte buffer of
     * the block size. If the given destination buffer is not null and the block
     * size is greater than the bytes remaining in the destination buffer, size
     * of the addressed block, no bytes are transferred and a
     * <code>BufferOverflowException</code> is thrown.
     * <p>
     * Returns the destination block given or created, or null if the block is
     * not found in page. The block might not be found if the block has been
     * moved. In this case, the caller is supposed to try dereferencing the
     * block address again. More on address races at {@link Mutator#tryRead}.
     * <p>
     * The block referenced by the given address is found by iterating through
     * the blocks in the page and finding the block with the given address as
     * its back-reference address.
     * <p>
     * This method synchronizes using the the underlying <code>RawPage</code>
     * object as a mutex.
     * 
     * @param address
     *            The block address to find.
     * @param destination
     *            The destination buffer or null to indicate that the method
     *            should allocate a destination buffer of block size.
     * @return The given or created destination buffer, or null if the the block
     *         is not found in the page.
     * @throws BufferOverflowException
     *             If the size of the block is greater than the bytes remaining
     *             in the destination buffer.
     */
    public ByteBuffer read(long address, ByteBuffer destination)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                if (destination == null)
                {
                    destination = ByteBuffer.allocateDirect(getBlockSize(address) - Pack.BLOCK_HEADER_SIZE);
                }
                int offset = bytes.position();
                int size = bytes.getInt();
                if (bytes.getLong() != address)
                {
                    throw new IllegalStateException();
                }
                bytes.limit(offset + size);
                destination.put(bytes);
                bytes.limit(bytes.capacity());
                return destination;
            }
        }
        return null;
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
    public boolean free(long address, DirtyPageSet dirtyPages)
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
    
                getRawPage().invalidate(offset, Pack.INT_SIZE);
                bytes.putInt(offset, size);
    
                dirtyPages.add(getRawPage());
                
                return true;
            }
        }
        
        return false;
    }

    /**
     * Frees a block on a block page used as an interim block page by copying
     * subsequent blocks over the gap created by the free block, freeing up the
     * entire remaining region of the block page for the allocation of new
     * blocks. This unallocates a block, rather than freeing it and potentially
     * leaving a gap with a freed block to skip.
     * <p>
     * Since an interim block page is only referenced by a single mutator, all
     * of the blocks belong to a single mutator, so copying over blocks will not
     * interfere with a concurrent attempt to read the other blocks.
     * 
     * @param address
     *            The address of the block to free.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    public void unallocate(long address, DirtyPageSet dirtyPages)
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
    
            getRawPage().invalidate(Pack.LONG_SIZE, Pack.INT_SIZE);
            bytes.putInt(Pack.LONG_SIZE, getDiskBlockCount());
        }
    }
}
