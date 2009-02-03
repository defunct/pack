package com.goodworkalan.pack;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;

import com.goodworkalan.sheaf.DirtyRegionMap;

final class Header extends DirtyRegionMap
{
    private final ByteBuffer bytes;
    
    public Header(ByteBuffer bytes)
    {
        super(0L);
        this.bytes = bytes;
    }
    
    public int getHeaderSize(Map<URI, Integer> staticBlocks)
    {
        int size = Pack.COUNT_SIZE;
        for (Map.Entry<URI, Integer> entry: staticBlocks.entrySet())
        {
            size += Pack.COUNT_SIZE + Pack.ADDRESS_SIZE;
            size += entry.getKey().toString().length() * 2;
        }
        size += getInternalJournalCount() * Pack.POSITION_SIZE;
        size += Pack.FILE_HEADER_SIZE;
        return size;
    }
    
    public ByteBuffer getByteBuffer()
    {
        return bytes;
    }
    
    public long getStaticPagesStart()
    {
        return Pack.FILE_HEADER_SIZE + getInternalJournalCount() * Pack.POSITION_SIZE;
    }

    // TODO Make this a checksum.
    public long getSignature()
    {
        return bytes.getLong(0);
    }
    
    public void setSignature(long signature)
    {
        bytes.putLong(0, signature);
        invalidate(0, Pack.CHECKSUM_SIZE);
    }
    
    public int getShutdown()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE);
    }
    
    public void setShutdown(int shutdown)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE, shutdown);
        invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
    }
    
    public int getPageSize()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE);
    }
    
    public void setPageSize(int pageSize)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE, pageSize);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE, Pack.COUNT_SIZE);
    }
    
    public int getAlignment()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 2);
    }
    
    public void setAlignment(int alignment)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 2, alignment);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 2, Pack.COUNT_SIZE);
    }
    
    public int getInternalJournalCount()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 3);
    }
    
    public void setInternalJournalCount(int internalJournalCount)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 3, internalJournalCount);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 3, Pack.COUNT_SIZE);
    }
    
    public int getStaticPageSize()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 4);
    }
    
    public void setStaticPageSize(int staticPageSize)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 4, staticPageSize);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 4, Pack.COUNT_SIZE);
    }
    
    public int getFirstAddressPageStart()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6);
    }
    
    public void setFirstAddressPageStart(int firstAddressPageStart)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6, firstAddressPageStart);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6, Pack.COUNT_SIZE);
    }

    public long getDataBoundary()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6);
    }
    
    public void setDataBoundary(long dataBoundary)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6, dataBoundary);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6, Pack.ADDRESS_SIZE);
    }
    
    // FIXME Rename.
    public long getOpenBoundary()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE * 2 + Pack.COUNT_SIZE * 6);
    }
    
    // FIXME Rename.
    public void setOpenBoundary(long openBoundary)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE * 2 + Pack.COUNT_SIZE * 6, openBoundary);
        invalidate(Pack.CHECKSUM_SIZE * 2 + Pack.COUNT_SIZE * 6, Pack.ADDRESS_SIZE);
    }
    
    public long getTemporaries()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE * 3 + Pack.COUNT_SIZE * 6);
    }
    
    public void setTemporaries(long temporaries)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE * 3 + Pack.COUNT_SIZE * 6, temporaries);
        invalidate(Pack.CHECKSUM_SIZE * 3 + Pack.COUNT_SIZE * 6, Pack.ADDRESS_SIZE);
    }
}