package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;

// TODO Comment.
class ByRemainingPage extends Page
{
    /** The size of an alignment record; position count and slot position. */ 
    private final static int RECORD_SIZE = Pack.INT_SIZE + Pack.LONG_SIZE;
    
    // TODO Comment.
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
    
    // TODO Comment.
    public void setAlignment(int alignment, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putInt(0, alignment);
    }
    
    // TODO Comment.
    public int getAlignment()
    {
        return getRawPage().getByteBuffer().getInt(0);
    }
    
    // TODO Comment.
    public int getSizeCount(int alignmentIndex)
    {
        return getRawPage().getByteBuffer().getInt(RECORD_SIZE * alignmentIndex);
    }
    
    // TODO Comment.
    public void increment(int alignmentIndex, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putInt(RECORD_SIZE * alignmentIndex, getSizeCount(alignmentIndex) + 1);
        getRawPage().invalidate(RECORD_SIZE * alignmentIndex, Pack.INT_SIZE);
    }
    
    // TODO Comment.
    public void decrement(int alignmentIndex, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putInt(RECORD_SIZE * alignmentIndex, getSizeCount(alignmentIndex) - 1);
        getRawPage().invalidate(RECORD_SIZE * alignmentIndex, Pack.INT_SIZE);
    }

    // TODO Comment.
    public long getSlotPosition(int alignmentIndex)
    {
        return getRawPage().getByteBuffer().getLong(RECORD_SIZE * alignmentIndex + Pack.INT_SIZE);
    }
    
    // TODO Comment.
    public void setSlotPosition(int alignmentIndex, long address, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putLong(RECORD_SIZE * alignmentIndex + Pack.INT_SIZE, address);
        getRawPage().invalidate(RECORD_SIZE * alignmentIndex + Pack.INT_SIZE, Pack.LONG_SIZE);
    }
    
    // TODO Comment.
    public long getAllocSlotPosition(int slotIndex)
    {
        int pageSize = getRawPage().getSheaf().getPageSize();
        int alignment = getAlignment();
        return getRawPage().getByteBuffer().getLong(RECORD_SIZE * (pageSize / alignment) + Pack.LONG_SIZE * slotIndex);
    }
    
    // TODO Comment.
    public void setAllocSlotPosition(int slotIndex, long address, DirtyPageSet dirtyPages)
    {
        int pageSize = getRawPage().getSheaf().getPageSize();
        int alignment = getAlignment();
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putLong(RECORD_SIZE * (pageSize / alignment) + Pack.LONG_SIZE * slotIndex, address);
        getRawPage().invalidate(RECORD_SIZE * (pageSize / alignment) + Pack.LONG_SIZE * slotIndex, Pack.LONG_SIZE);
    }
}
