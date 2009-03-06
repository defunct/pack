package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;
import com.goodworkalan.sheaf.RawPage;

/**
 * A page that references linked lists of slots that reference pages with bytes
 * remaining for allocation, that also references the pages from which the slots
 * are allocated.
 * 
 * @author Alan Gutierrez
 */
class ByRemainingPage extends Page
{
    /**
     * Create a by remaining page by writing zero to the entire new page.
     * 
     * @param dirtyPages
     *            The dirty page set.
     */
    @Override
    public void create(DirtyPageSet dirtyPages)
    {
        RawPage rawPage = getRawPage_();
        dirtyPages.add(rawPage);
        ByteBuffer byteBuffer = rawPage.getByteBuffer();
        while (byteBuffer.remaining() == 0)
        {
            byteBuffer.put((byte) 0);
        }
        rawPage.dirty();
    }

    /**
     * Set the alignment to which all block allocations are rounded.
     * 
     * @param alignment
     *            The block alignment.
     * @dirtyPages The dirty page set.
     */
    public void setAlignment(int alignment, DirtyPageSet dirtyPages)
    {
        RawPage rawPage = getRawPage_();
        rawPage.getLock().lock();
        try
        {
            dirtyPages.add(rawPage);
            rawPage.getByteBuffer().putInt(0, alignment);
        }
        finally
        {
            rawPage.getLock().unlock();
        }
    }
    
    /**
     * Get the alignment to which all block allocations are rounded.
     * 
     * @return The block alignment.
     */
    public int getAlignment()
    {
        RawPage rawPage = getRawPage_();
        rawPage.getLock().lock();
        try
        {
            return rawPage.getByteBuffer().getInt(0);
        }
        finally
        {
            rawPage.getLock().unlock();
        }
    }

    /**
     * Get the first slot in a linked list of slots that store page positions
     * with the aligned bytes remaining for allocation indicated by the given
     * index. Returns zero if no list has yet been allocated.
     * 
     * @param alignmentIndex
     *            The alignment index.
     * @return The position of the first slot in a linked list of slots or zero if no list
     *         exists.
     */
    public long getSlotPosition(int alignmentIndex)
    {
        RawPage rawPage = getRawPage_();
        rawPage.getLock().lock();
        try
        {
            return rawPage.getByteBuffer().getLong(Pack.LONG_SIZE * alignmentIndex);
        }
        finally
        {
            rawPage.getLock().unlock();
        }
    }

    /**
     * Set the first slot in a linked list of slots that store page positions
     * with the aligned bytes remaining for allocation indicated by the given
     * index.
     * 
     * @param alignmentIndex
     *            The alignment index.
     * @param address
     *            The position of the first slot in a linked list of slots or
     *            zero if no list exists.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void setSlotPosition(int alignmentIndex, long address, DirtyPageSet dirtyPages)
    {
        RawPage rawPage = getRawPage_();
        rawPage.getLock().lock();
        try
        {
            dirtyPages.add(rawPage);
            rawPage.getByteBuffer().putLong(Pack.LONG_SIZE * alignmentIndex, address);
            rawPage.dirty(Pack.LONG_SIZE * alignmentIndex, Pack.LONG_SIZE);
        }
        finally
        {
            rawPage.getLock().unlock();
        }
    }

    /**
     * Get the page from which slots of the size indicated by the given slot
     * index are allocated. Returns zero if no such page has yet been allocated.
     * 
     * @param slotIndex
     *            The slot index.
     * @return The page from which slots of the size indicated by the given slot
     *         index are allocated or zero if none exists.
     */
    public long getAllocSlotPosition(int slotIndex)
    {
        int alignment = getAlignment();
        RawPage rawPage = getRawPage_();
        int pageSize = rawPage.getSheaf().getPageSize();
        rawPage.getLock().lock();
        try
        {
            return rawPage.getByteBuffer().getLong(Pack.LONG_SIZE * (pageSize / alignment) + Pack.LONG_SIZE * slotIndex);
        }
        finally
        {
            rawPage.getLock().unlock();
        }
    }

    /**
     *Set the page from which slots of the size indicated by the given slot
     * index are allocated.
     * 
     * @param slotIndex
     *            The slot index.
     * @param position
     *            The position of the page from which to allocate slots of the
     *            size indicated by the slot index.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void setAllocSlotPosition(int slotIndex, long position, DirtyPageSet dirtyPages)
    {
        int alignment = getAlignment();
        RawPage rawPage = getRawPage_();
        int pageSize = rawPage.getSheaf().getPageSize();
        rawPage.getLock().lock();
        try
        {
            dirtyPages.add(rawPage);
            rawPage.getByteBuffer().putLong(Pack.LONG_SIZE * (pageSize / alignment) + Pack.LONG_SIZE * slotIndex, position);
            rawPage.dirty(Pack.LONG_SIZE * (pageSize / alignment) + Pack.LONG_SIZE * slotIndex, Pack.LONG_SIZE);
        }
        finally
        {
            rawPage.getLock().unlock();
        }
    }
}
