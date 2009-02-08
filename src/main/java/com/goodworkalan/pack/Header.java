package com.goodworkalan.pack;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;

import com.goodworkalan.sheaf.DirtyRegionMap;

/**
 * Reads and writes the header fields of a pack file. The header is built on top
 * of {@link DirtyRegionMap} so that writing header fields will only cause the
 * dirty fields to be written when the header is flushed.
 * 
 * @author Alan Gutierrez
 */
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

    /**
     * Get the size of the pack file header including the set of named static
     * pages and the journal headers.
     * 
     * @return The size of the file header.
     */
    public int getHeaderSize()
    {
        return bytes.getInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6);
    }

    /**
     * Set the size of the pack file header including the set of named static
     * pages and the journal headers.
     * 
     * @param headerSize
     *            The size of the file header.
     */    
    public void setHeaderSize(int headerSize)
    {
        bytes.putInt(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6, headerSize);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6, Pack.COUNT_SIZE);
    }

    /**
     * Get the user boundary of the pack file. The user boundary is the 
     * position of the first user page, less the file header offset.
     * 
     * @return The position of the first user page.
     */
    public long getUserBoundary()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6);
    }

    /**
     * Set the user boundary of the pack file. The user boundary is the position
     * of the first user page, less the file header offset.
     * 
     * @param userBoundary
     *            The position of the first user page.
     */
    public void setUserBoundary(long userBoundary)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6, userBoundary);
        invalidate(Pack.CHECKSUM_SIZE + Pack.COUNT_SIZE * 6, Pack.ADDRESS_SIZE);
    }

    /**
     * Get the interim boundary of the pack file, which in the case of a closed
     * file that was shutdown softly, is the beginning of the storage of the
     * free page information. The interim boundary is position of the first
     * interim page, less the file header offset.
     * 
     * @return The position of the interim boundary.
     */
    public long getInterimBoundary()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE * 2 + Pack.COUNT_SIZE * 6);
    }

    /**
     * Get the interim boundary of the pack file, which in the case of a closed
     * file that was shutdown softly, is the beginning of the storage of the
     * free page information. The interim boundary is position of the first
     * interim page, less the file header offset.
     * 
     * @param interimBoundary
     *            The position of the interim boundary.
     */
    public void setInterimBoundary(long interimBoundary)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE * 2 + Pack.COUNT_SIZE * 6, interimBoundary);
        invalidate(Pack.CHECKSUM_SIZE * 2 + Pack.COUNT_SIZE * 6, Pack.ADDRESS_SIZE);
    }

    /**
     * Get the file position of the first temporary block node in a linked list
     * of temporary block nodes.
     * 
     * @return The position of the first temporary block node.
     */
    public long getFirstTemporaryNode()
    {
        return bytes.getLong(Pack.CHECKSUM_SIZE * 3 + Pack.COUNT_SIZE * 6);
    }

    /**
     * Set the file position of the first temporary block node in a linked list
     * of temporary block nodes.
     * 
     * @param temporaries
     *            The position of the first temporary block node.
     */  
    public void setFirstTemporaryNode(long temporaries)
    {
        bytes.putLong(Pack.CHECKSUM_SIZE * 3 + Pack.COUNT_SIZE * 6, temporaries);
        invalidate(Pack.CHECKSUM_SIZE * 3 + Pack.COUNT_SIZE * 6, Pack.ADDRESS_SIZE);
    }
}