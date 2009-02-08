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
abstract class BlockPage extends RelocatablePage
{
    /** The count of blocks in this page. */
    protected int count;

    /** The count of bytes remaining for block allocation. */
    protected int remaining;

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
     * Return an integer value to write to disk to store the block count. User
     * block pages store a negative value to indicate block count, interim pages
     * store a positive value.
     * <p>
     * TODO Why so clever? We got rid of checksums. That is eight bytes saved.
     * Why not use the first byte, short or integer as a set of flags instead?
     * <p>
     * Guess we won't know until recovery is implemented.
     */
    protected abstract int getDiskCount();

    /**
     * Convert an integer value read from to disk to store the block count into
     * an actual block count. User block pages store a negative value to
     * indicate block count, interim pages store a positive value.
     */
    protected abstract int convertDiskCount(int count);

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
        this.remaining = getRawPage().getSheaf().getPageSize()
                - Pack.BLOCK_PAGE_HEADER_SIZE;

        ByteBuffer bytes = getRawPage().getByteBuffer();

        bytes.clear();

        getRawPage().invalidate(0, Pack.BLOCK_PAGE_HEADER_SIZE);
        bytes.putLong(0L);
        bytes.putInt(getDiskCount());

        dirtyPages.add(getRawPage());
    }

    /**
     * Return the count of bytes allocated by iterating over the blocks
     * and reading their block sizes.
     * 
     * @return The count of bytes allocated.
     */
    private int getAllocated()
    {
        int consumed = Pack.BLOCK_PAGE_HEADER_SIZE;
        ByteBuffer bytes = getBlockRange();
        for (int i = 0; i < count; i++)
        {
            int size = getBlockSize(bytes);
            if (size > 0)
            {
                consumed += size;
            }
            advance(bytes, size);
        }
        return consumed;
    }

    /**
     * Load the block page from the underlying raw page.
     */
    public void load()
    {
        ByteBuffer bytes = getRawPage().getByteBuffer();

        bytes.clear();
        bytes.getLong();

        this.count = convertDiskCount(bytes.getInt());
        this.remaining = getRawPage().getSheaf().getPageSize() - getAllocated();
    }

    /**
     * Get the count of blocks in this page.
     * <p>
     * FIXME Rename getBlockCount.
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
        return bytes.getLong(bytes.position() + Pack.COUNT_SIZE);
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
        int block = 0;
        while (block < count)
        {
            int size = getBlockSize(bytes);
            if (size > 0)
            {
                block++;
            }
            if (getAddress(bytes) == address)
            {
                return true;
            }
            advance(bytes, size);
        }
        return false;
    }

    /**
     * Get the full block size including the block header of the block in this
     * block page at the given address.
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
        throw new IllegalArgumentException();
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
            int block = 0;
            while (block < getBlockCount())
            {
                int size = getBlockSize(bytes);
                if (size > 0)
                {
                    block++;
                    listOfAddresses.add(getAddress(bytes));
                }
                advance(bytes, size);
            }
        }
        return listOfAddresses;
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
                    throw new PackException(Pack.ERROR_BLOCK_PAGE_CORRUPT);
                }
                bytes.limit(offset + size);
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
                    destination = ByteBuffer
                            .allocateDirect(getBlockSize(address)
                                    - Pack.BLOCK_HEADER_SIZE);
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
}
