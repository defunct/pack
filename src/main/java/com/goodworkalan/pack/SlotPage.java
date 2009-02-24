package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;

/**
 * A page for managing slots of file regions containing file positions in a
 * doubly linked list of said slots.
 * 
 * @author Alan Gutierrez
 */
class SlotPage extends Page
{
    /**
     * Create a by remaining slot page by writing zero to the entire new page.
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
        getRawPage().invalidate(0, getRawPage().getSheaf().getPageSize());
    }

    /**
     * Set the size of a slot in this page as a count of longs.
     * 
     * @param slotSize
     *            The slot size.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void setSlotSize(int slotSize, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putInt(0, slotSize);
        getRawPage().invalidate(0, Pack.INT_SIZE);
    }
    
    /**
     * Get the size of a slot in this page as a count of longs.
     * 
     * @return The slot size.
     */
    public int getSlotSize()
    {
        return getRawPage().getByteBuffer().getInt(0);
    }

    /**
     * Add the given position to the set of positions at the given set position.
     * Returns true if the position is added, false if the set is full.
     * 
     * @param slotPosition
     *            The position of start the set on disk.
     * @param position
     *            The file position.
     * @param dirtyPages
     *            The dirty page set.
     * @return True if the position was added, false if the set is full.
     */
    public boolean add(long slotPosition, long position, boolean force, DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int offset = (int) (slotPosition - getRawPage().getPosition());
        int slotSize = getSlotSize();
        if (!force && byteBuffer.getLong(offset + (slotSize - 1) * Pack.LONG_SIZE) != 0L)
        {
            return false;
        }

        int i;
        for (i = offset + Pack.LONG_SIZE * 2; byteBuffer.getLong(i) < position; i += Pack.LONG_SIZE)
        {
            if (byteBuffer.getLong(i - Pack.LONG_SIZE) == 0L)
            {
                byteBuffer.putLong(i - Pack.LONG_SIZE, byteBuffer.getLong(i));
                byteBuffer.putLong(i, 0L);
                getRawPage().invalidate(i - Pack.LONG_SIZE, Pack.LONG_SIZE * 2);
            }
        }
        
        if (byteBuffer.getLong(i - Pack.LONG_SIZE) == 0L)
        {
            byteBuffer.putLong(i - Pack.LONG_SIZE, byteBuffer.getLong(i));
            byteBuffer.putLong(i, position);
            getRawPage().invalidate(i - Pack.LONG_SIZE, Pack.LONG_SIZE * 2);
            return true;
        }
        
        int j;
        for (j = i; byteBuffer.getLong(i) != 0L; i += Pack.LONG_SIZE)
        {
        }

        getRawPage().invalidate(i, j - i);
        for (; j != i; j -= Pack.LONG_SIZE)
        {
            byteBuffer.putLong(j - Pack.LONG_SIZE, byteBuffer.getLong(j));
        }

        byteBuffer.putLong(i, position);
        
        return true;
    }

    /**
     * Remove and return a value from the set at the given set position or zero
     * if there are not values in this set. If a value is removed from the page,
     * this page is added to the dirty page set.
     * 
     * @param slotPosition
     *            The position of start the set on disk.
     * @param dirtyPages
     *            The dirty page set.
     * @return
     */
    public long remove(long slotPosition, DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int offset = (int) (slotPosition - getRawPage().getPosition());
        int slotSize = getSlotSize();
        for (int i = slotSize - Pack.LONG_SIZE; i != offset + Pack.LONG_SIZE; i -= Pack.LONG_SIZE)
        {
            long value = byteBuffer.getLong(offset);
            if (value != 0L)
            {
                dirtyPages.add(getRawPage());
                byteBuffer.putLong(offset, 0L);
                getRawPage().invalidate(offset, Pack.LONG_SIZE);
                return value;
            }
        }
        return 0L;
    }

    /**
     * Remove a given position from the slot at the given slot position if it
     * exists in the slot. Return true if the position existed and was removed,
     * false if it did not exist.
     * 
     * @param slotPosition
     *            The slot position.
     * @param position
     *            The file position.
     * @param dirtyPages
     *            The dirty page set.
     * @return True if the position existed and was removed, false if it did not
     *         exist.
     */
    public boolean remove(long slotPosition, long position, DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int offset = (int) (slotPosition - getRawPage().getPosition());
        int slotSize = getSlotSize();
        int low = 0;
        int j;
        for (j = offset + slotSize * Pack.LONG_SIZE; byteBuffer.getLong(j) == 0L; j -= Pack.LONG_SIZE)
        {
        }
        int high = (j - offset) - 2;
        int mid = -1;
        long value = 0L;
        while (low <= high)
        {
            mid = low + ((high - low) / 2);
            value = byteBuffer.getLong(offset + Pack.LONG_SIZE * (2 + mid));
            if (value > position)
            {
                high = mid - 1;
            }
            else if (value < position)
            {
                low = mid + 1;
            }
            else
            {
                break;
            }
        }
        if (value == position)
        {
            dirtyPages.add(getRawPage());
            byteBuffer.putLong(offset + Pack.LONG_SIZE * (2 + mid), 0L);
            getRawPage().invalidate(offset + Pack.LONG_SIZE * (2 + mid), Pack.LONG_SIZE);
            return true;
        }
        return false;
    }

    /**
     * Return the smallest page position value in the slot indicated by the
     * given slot position. Returns zero if the slot is empty.
     * 
     * @param slotPosition
     *            The slot position.
     * @return The smallest page position value in the slot or zero of the slot
     *         is empty.
     */
    public long least(long slotPosition)
    {
        int offset = (int) (slotPosition - getRawPage().getPosition());
        return getRawPage().getByteBuffer().getLong(offset + Pack.LONG_SIZE * 2);
    }

    /**
     * Cleanup after the removal of a single slot position by overwriting the
     * empty slot by shifting all the following slots by one slot position
     * toward the start of the slot array.
     * 
     * @param slotPosition
     *            The slot position.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void compact(long slotPosition, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int offset = (int) (slotPosition - getRawPage().getPosition());
        int i;
        for (i = offset + Pack.LONG_SIZE * 2; byteBuffer.getLong(i) != 0L; i += Pack.LONG_SIZE)
        {
            if (byteBuffer.getLong(i - Pack.LONG_SIZE) == 0L)
            {
                byteBuffer.putLong(i - Pack.LONG_SIZE, byteBuffer.getLong(i));
                byteBuffer.putLong(i, 0L);
                getRawPage().invalidate(i - Pack.LONG_SIZE, Pack.LONG_SIZE * 2);
            }
        }
    }

    /**
     * Set the previous set position of the set at the given set position to the
     * given previous set position. The sets participate in a linked list of
     * sets for a specific alignment.
     * 
     * @param slotPosition
     *            The position of start the set on disk.
     * @param previous
     *            The position of the previous set.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void setPrevious(long slotPosition, long previous, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        int offset = (int) (slotPosition - getRawPage().getPosition());
        getRawPage().getByteBuffer().putLong(offset, previous);
        getRawPage().invalidate(offset, Pack.LONG_SIZE);
    }

    /**
     * Get the previous set position of the set at the given set position. The
     * sets participate in a linked list of sets for a specific alignment.
     * 
     * @param slotPosition
     *            The slot position on disk.
     * @return The previous slot position.
     */
    public long getPrevious(long slotPosition)
    {
        int offset = (int) (slotPosition - getRawPage().getPosition());
        return getRawPage().getByteBuffer().getLong(offset);
    }

    /**
     * Set the next set position of the set at the given set position to the
     * given next set position. The sets participate in a linked list of sets
     * for a specific alignment.
     * 
     * @param slotPosition
     *            The position of start the set on disk.
     * @param next
     *            The next of the previous set.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void setNext(long slotPosition, long next, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        int offset = (int) (slotPosition - getRawPage().getPosition());
        getRawPage().getByteBuffer().putLong(offset+ Pack.LONG_SIZE, next);
        getRawPage().invalidate(offset + Pack.LONG_SIZE, Pack.LONG_SIZE);
    }

    /**
     * Get the previous set position of the set at the given set position. The
     * sets participate in a linked list of sets for a specific alignment.
     * 
     * @param slotPosition
     *            The slot position on disk.
     * @return The previous slot position.
     */
    public long getNext(long slotPosition)
    {
        int offset = (int) (slotPosition - getRawPage().getPosition());
        return getRawPage().getByteBuffer().getLong(offset + Pack.LONG_SIZE);
    }

    /**
     * Allocate a new slot from this page whose previous slot is set to the
     * given previous slot position. Returns the position of the newly allocated
     * slot or zero of there are no more slots available in this page.
     * 
     * @param previous
     *            The previous slot.
     * @param dirtyPages
     *            The dirty page set.
     * @return The address of the newly allocated slot or zero.
     */
    public long allocateSlot(long previous, DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int pageSize = getRawPage().getSheaf().getPageSize();
        int slotSize = getSlotSize() * Pack.LONG_SIZE;
        for (int i = Pack.INT_SIZE; i + slotSize < pageSize; i += slotSize)
        {
            if (byteBuffer.getLong(i) == 0L)
            {
                dirtyPages.add(getRawPage());
                getRawPage().invalidate(i, Pack.LONG_SIZE);
                byteBuffer.putLong(i, previous);
                byteBuffer.putLong(i +  Pack.LONG_SIZE , Long.MIN_VALUE);
                return getRawPage().getPosition() + i;
            }
        }
        return 0L;
    }

    /**
     * Remove a slot from the slot page returning an array of the the slot
     * contents including the previous and next slot positions. Returns null if
     * there are no slots allocated in this page.
     * 
     * @param dirtyPages
     *            The dirty page set.
     * @return The contents of the removed slot or null if there are no slots.
     */
    public long[] removeSlot(DirtyPageSet dirtyPages)
    {
        ByteBuffer byteBuffer = getRawPage().getByteBuffer();
        int pageSize = getRawPage().getSheaf().getPageSize();
        int slotSize = getSlotSize() * Pack.LONG_SIZE;
        for (int i = Pack.INT_SIZE; i + slotSize < pageSize; i += slotSize)
        {
            if (byteBuffer.getLong(i) != 0L)
            {
                dirtyPages.add(getRawPage());
                long[] values = new long[getSlotSize()];
                for (int j = i; j < getSlotSize(); j++)
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
