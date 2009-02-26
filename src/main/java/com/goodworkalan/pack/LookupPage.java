package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;

/**
 * A page for managing sorted blocks of long values in a doubly linked list of
 * said blocks.
 * 
 * @author Alan Gutierrez
 */
class LookupPage extends Page
{
    /**
     * Create a long page by writing zero to the entire new page.
     * 
     * @param dirtyPages
     *            The dirty page set.
     */
    @Override
    public void create(DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        while (byteBuffer.remaining() == 0)
        {
            byteBuffer.put((byte) 0);
        }
        getRawPage().dirty(0, getRawPage().getSheaf().getPageSize());
    }

    /**
     * Set the size of a block in this page as a count of longs.
     * 
     * @param blockSize
     *            The block size.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void setBlockSize(int blockSize, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putInt(0, blockSize);
        getRawPage().dirty(0, Pack.INT_SIZE);
    }
    
    /**
     * Get the size of a block of longs in this page.
     * 
     * @return The block size.
     */
    public int getBlockSize()
    {
        return getRawPage().getByteBuffer().getInt(0);
    }

    /**
     * Add the given long value to the block of long values at the given block
     * position. Returns true if the long value is added, false if the block is
     * full.
     * 
     * @param position
     *            The position of start the block on disk.
     * @param value
     *            The long value.
     * @param dirtyPages
     *            The dirty page set.
     * @return True if the position was added, false if the set is full.
     */
    public boolean add(long position, long value, boolean force, DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int offset = (int) (position - getRawPage().getPosition());
        int blockSize = getBlockSize();
        if (!force && byteBuffer.getLong(offset + (blockSize - 1) * Pack.LONG_SIZE) != 0L)
        {
            return false;
        }

        int i;
        for (i = offset + Pack.LONG_SIZE * 2; byteBuffer.getLong(i) < value; i += Pack.LONG_SIZE)
        {
            if (byteBuffer.getLong(i - Pack.LONG_SIZE) == 0L)
            {
                byteBuffer.putLong(i - Pack.LONG_SIZE, byteBuffer.getLong(i));
                byteBuffer.putLong(i, 0L);
                getRawPage().dirty(i - Pack.LONG_SIZE, Pack.LONG_SIZE * 2);
            }
        }
        
        if (byteBuffer.getLong(i - Pack.LONG_SIZE) == 0L)
        {
            byteBuffer.putLong(i - Pack.LONG_SIZE, byteBuffer.getLong(i));
            byteBuffer.putLong(i, value);
            getRawPage().dirty(i - Pack.LONG_SIZE, Pack.LONG_SIZE * 2);
            return true;
        }
        
        int j;
        for (j = i; byteBuffer.getLong(i) != 0L; i += Pack.LONG_SIZE)
        {
        }

        getRawPage().dirty(i, j - i);
        for (; j != i; j -= Pack.LONG_SIZE)
        {
            byteBuffer.putLong(j - Pack.LONG_SIZE, byteBuffer.getLong(j));
        }

        byteBuffer.putLong(i, value);
        
        return true;
    }

    /**
     * Remove and return a long value from the block of long values at the given
     * block position or zero if there are no values in the block. If a value is
     * removed from the page, this page is added to the dirty page set.
     * 
     * @param position
     *            The position of start the block on disk.
     * @param dirtyPages
     *            The dirty page set.
     * @return The removed long value or zero of the block is empty.
     */
    public long remove(long position, DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int offset = (int) (position - getRawPage().getPosition());
        int blockSize = getBlockSize();
        for (int i = blockSize - Pack.LONG_SIZE; i != offset + Pack.LONG_SIZE; i -= Pack.LONG_SIZE)
        {
            long value = byteBuffer.getLong(offset);
            if (value != 0L)
            {
                dirtyPages.add(getRawPage());
                byteBuffer.putLong(offset, 0L);
                getRawPage().dirty(offset, Pack.LONG_SIZE);
                return value;
            }
        }
        return 0L;
    }

    /**
     * Remove a given long value from the block of long values at the given
     * block position if the long value exists in the block. Return true if the
     * long value existed and was removed, false if it did not exist in the
     * block.
     * 
     * @param position
     *            The position of the block.
     * @param value
     *            The file position.
     * @param dirtyPages
     *            The dirty page set.
     * @return True if the long value existed and was removed, false if it did
     *         not exist in the block.
     */
    public boolean remove(long position, long value, DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int offset = (int) (position - getRawPage().getPosition());
        int slotSize = getBlockSize();
        int low = 0;
        int j;
        for (j = offset + slotSize * Pack.LONG_SIZE; byteBuffer.getLong(j) == 0L; j -= Pack.LONG_SIZE)
        {
        }
        int high = (j - offset) - 2;
        int mid = -1;
        long found = 0L;
        while (low <= high)
        {
            mid = low + ((high - low) / 2);
            found = byteBuffer.getLong(offset + Pack.LONG_SIZE * (2 + mid));
            if (found > value)
            {
                high = mid - 1;
            }
            else if (found < value)
            {
                low = mid + 1;
            }
            else
            {
                break;
            }
        }
        if (found == value)
        {
            dirtyPages.add(getRawPage());
            byteBuffer.putLong(offset + Pack.LONG_SIZE * (2 + mid), 0L);
            getRawPage().dirty(offset + Pack.LONG_SIZE * (2 + mid), Pack.LONG_SIZE);
            return true;
        }
        return false;
    }

    /**
     * Return the smallest long value in the block of long values indicated by
     * the given position. Returns zero if the block is empty.
     * 
     * @param block
     *            The block position.
     * @return The smallest page long value in the block or zero of the block
     *         is empty.
     */
    public long least(long block)
    {
        int offset = (int) (block - getRawPage().getPosition());
        return getRawPage().getByteBuffer().getLong(offset + Pack.LONG_SIZE * 2);
    }

    /**
     * Cleanup after the removal of a long value by overwriting the empty long
     * value by shifting all the following long values by one long value toward
     * the start of the slot array.
     * 
     * @param position
     *            The block position.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void compact(long position, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int offset = (int) (position - getRawPage().getPosition());
        int i;
        for (i = offset + Pack.LONG_SIZE * 2; byteBuffer.getLong(i) != 0L; i += Pack.LONG_SIZE)
        {
            if (byteBuffer.getLong(i - Pack.LONG_SIZE) == 0L)
            {
                byteBuffer.putLong(i - Pack.LONG_SIZE, byteBuffer.getLong(i));
                byteBuffer.putLong(i, 0L);
                getRawPage().dirty(i - Pack.LONG_SIZE, Pack.LONG_SIZE * 2);
            }
        }
    }

    /**
     * Set the previous block position of the block of long values at the given
     * block position to the given previous block position.
     * 
     * @param position
     *            The position of start the set on disk.
     * @param previous
     *            The position of the previous set.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void setPrevious(long position, long previous, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        int offset = (int) (position - getRawPage().getPosition());
        getRawPage().getByteBuffer().putLong(offset, previous);
        getRawPage().dirty(offset, Pack.LONG_SIZE);
    }

    /**
     * Get the previous block position of the block of long values at the given
     * block position.
     * 
     * @param position
     *            The block position.
     * @return The previous block position.
     */
    public long getPrevious(long position)
    {
        int offset = (int) (position - getRawPage().getPosition());
        return getRawPage().getByteBuffer().getLong(offset);
    }

    /**
     * Set the previous block position of the block of long values at the given
     * block position.
     * 
     * @param position
     *           The block position.
     * @param next
     *            The position of the next block.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void setNext(long position, long next, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        int offset = (int) (position - getRawPage().getPosition());
        getRawPage().getByteBuffer().putLong(offset+ Pack.LONG_SIZE, next);
        getRawPage().dirty(offset + Pack.LONG_SIZE, Pack.LONG_SIZE);
    }

    /**
     * Get the previous block position of the block of long values at the given
     * block position.
     * 
     * @param position
     *            The block position.
     * @return The previous block position.
     */
    public long getNext(long position)
    {
        int offset = (int) (position - getRawPage().getPosition());
        return getRawPage().getByteBuffer().getLong(offset + Pack.LONG_SIZE);
    }

    /**
     * Allocate a new block of longs from this page whose previous block is set
     * to the given previous block position. Returns the position of the newly
     * allocated block of longs or zero of there are no more blocks available in
     * this page.
     * 
     * @param previous
     *            The previous slot.
     * @param dirtyPages
     *            The dirty page set.
     * @return The address of the newly allocated block or zero.
     */
    public long allocateBlock(long previous, DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int pageSize = getRawPage().getSheaf().getPageSize();
        int slotSize = getBlockSize() * Pack.LONG_SIZE;
        for (int i = Pack.INT_SIZE; i + slotSize < pageSize; i += slotSize)
        {
            if (byteBuffer.getLong(i) == 0L)
            {
                dirtyPages.add(getRawPage());
                getRawPage().dirty(i, Pack.LONG_SIZE);
                byteBuffer.putLong(i, previous);
                byteBuffer.putLong(i +  Pack.LONG_SIZE , Long.MIN_VALUE);
                return getRawPage().getPosition() + i;
            }
        }
        return 0L;
    }

    /**
     * Remove a block of longs from the slot long returning an array of the the
     * long values including the previous and next long block positions. Returns
     * null if there are no blocks allocated in this page.
     * 
     * @param dirtyPages
     *            The dirty page set.
     * @return The contents of the removed block or null if there are no blocks.
     */
    public long[] removeBlock(DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int pageSize = getRawPage().getSheaf().getPageSize();
        int blockSize = getBlockSize() * Pack.LONG_SIZE;
        for (int i = Pack.INT_SIZE; i + blockSize < pageSize; i += blockSize)
        {
            if (byteBuffer.getLong(i) != 0L)
            {
                dirtyPages.add(getRawPage());
                long[] values = new long[getBlockSize()];
                for (int j = i; j < getBlockSize(); j++)
                {
                    values[j] = byteBuffer.getLong(i + j * Pack.LONG_SIZE);
                    byteBuffer.putLong(i + j * Pack.LONG_SIZE, 0L);
                }
                return values;
            }            
        }
        return null;
    }
}
