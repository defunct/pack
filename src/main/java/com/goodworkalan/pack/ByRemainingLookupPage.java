package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;

public class ByRemainingLookupPage extends Page
{
    private final static int INT_SIZE = (Integer.SIZE / Byte.SIZE);
    
    private final static int LONG_SIZE = (Long.SIZE / Byte.SIZE);

    private final static int RECORD_SIZE = INT_SIZE + LONG_SIZE;
    
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
    
    public void setAlignment(int alignment, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putInt(0, alignment);
    }
    
    public int getAlignment()
    {
        return getRawPage().getByteBuffer().getInt(0);
    }
    
    public int getSizeCount(int index)
    {
        return getRawPage().getByteBuffer().getInt(RECORD_SIZE * index);
    }
    
    public void increment(int index, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putInt(RECORD_SIZE * index, getSizeCount(index) + 1);
        getRawPage().invalidate(RECORD_SIZE * index, INT_SIZE);
    }
    
    public void deccrement(int index, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putInt(RECORD_SIZE * index, getSizeCount(index) - 1);
        getRawPage().invalidate(RECORD_SIZE * index, INT_SIZE);
    }

    public long getSetPosition(int index)
    {
        return getRawPage().getByteBuffer().getLong(RECORD_SIZE * index + INT_SIZE);
    }
    
    public void setSetPosition(int index, long address, DirtyPageSet dirtyPages)
    {
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putLong(RECORD_SIZE * index + INT_SIZE, address);
        getRawPage().invalidate(RECORD_SIZE * index + INT_SIZE, LONG_SIZE);
    }
    
    public long getFreeSetPosition(int index)
    {
        int pageSize = getRawPage().getSheaf().getPageSize();
        int alignment = getAlignment();
        return getRawPage().getByteBuffer().getLong(RECORD_SIZE * (pageSize / alignment) + Pack.LONG_SIZE * index);
    }
    
    public void setFreeSetPosition(int index, long address, DirtyPageSet dirtyPages)
    {
        int pageSize = getRawPage().getSheaf().getPageSize();
        int alignment = getAlignment();
        dirtyPages.add(getRawPage());
        getRawPage().getByteBuffer().putLong(RECORD_SIZE * (pageSize / alignment) + Pack.LONG_SIZE * index, address);
        getRawPage().invalidate(RECORD_SIZE * (pageSize / alignment) + Pack.LONG_SIZE * index, Pack.LONG_SIZE);
    }
}
