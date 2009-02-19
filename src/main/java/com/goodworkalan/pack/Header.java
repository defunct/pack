package com.goodworkalan.pack;

import java.nio.ByteBuffer;

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
    /** The contents of the header region of the file. */
    private final ByteBuffer bytes;
    
    /**
     * Create a file header from the given content buffer.
     * 
     * @param bytes
     *            The contents of the header region of the file.
     */
    public Header(ByteBuffer bytes)
    {
        super(0L);
        this.bytes = bytes;
    }
    
    // TODO Comment.
    public ByteBuffer getByteBuffer()
    {
        return bytes;
    }
    
    // TODO Comment.
    public long getStaticPagesStart()
    {
        return Pack.FILE_HEADER_SIZE + getInternalJournalCount() * Pack.LONG_SIZE;
    }

    // TODO Comment.
    public long getSignature()
    {
        return bytes.getLong(0);
    }
    
    // TODO Comment.
    public void setSignature(long signature)
    {
        bytes.putLong(0, signature);
        invalidate(0, Pack.LONG_SIZE);
    }
    
    // TODO Comment.
    public int getShutdown()
    {
        return bytes.getInt(Pack.LONG_SIZE);
    }
    
    // TODO Comment.
    public void setShutdown(int shutdown)
    {
        bytes.putInt(Pack.LONG_SIZE, shutdown);
        invalidate(Pack.LONG_SIZE, Pack.INT_SIZE);
    }
    
    // TODO Comment.
    public int getPageSize()
    {
        return bytes.getInt(Pack.LONG_SIZE + Pack.INT_SIZE);
    }
    
    // TODO Comment.
    public void setPageSize(int pageSize)
    {
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE, pageSize);
        invalidate(Pack.LONG_SIZE + Pack.INT_SIZE, Pack.INT_SIZE);
    }
    
    /**
     * Get the alignment to which all block allocations are rounded.
     * 
     * @return The block alignment.
     */
    public int getAlignment()
    {
        return bytes.getInt(Pack.LONG_SIZE + Pack.INT_SIZE * 2);
    }
    
    /**
     * Set the alignment to which all block allocations are rounded.
     * 
     * @param alignment The block alignment.
     */
    public void setAlignment(int alignment)
    {
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE * 2, alignment);
        invalidate(Pack.LONG_SIZE + Pack.INT_SIZE * 2, Pack.INT_SIZE);
    }
    
    // TODO Comment.
    public int getInternalJournalCount()
    {
        return bytes.getInt(Pack.LONG_SIZE + Pack.INT_SIZE * 3);
    }
    
    // TODO Comment.
    public void setInternalJournalCount(int internalJournalCount)
    {
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE * 3, internalJournalCount);
        invalidate(Pack.LONG_SIZE + Pack.INT_SIZE * 3, Pack.INT_SIZE);
    }
    
    // TODO Comment.
    public int getStaticPageSize()
    {
        return bytes.getInt(Pack.LONG_SIZE + Pack.INT_SIZE * 4);
    }
    
    // TODO Comment.
    public void setStaticPageSize(int staticPageSize)
    {
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE * 4, staticPageSize);
        invalidate(Pack.LONG_SIZE + Pack.INT_SIZE * 4, Pack.INT_SIZE);
    }

    /**
     * Get the size of the pack file header including the set of named static
     * pages and the journal headers.
     * 
     * @return The size of the file header.
     */
    public int getHeaderSize()
    {
        return bytes.getInt(Pack.LONG_SIZE + Pack.INT_SIZE * 5);
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
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE * 5, headerSize);
        invalidate(Pack.LONG_SIZE + Pack.INT_SIZE * 5, Pack.INT_SIZE);
    }
    

    /**
     * Get the size of the pack file header including the set of named static
     * pages and the journal headers.
     * 
     * @return The size of the file header.
     */
    public int getAddressPagePoolSize()
    {
        return bytes.getInt(Pack.LONG_SIZE + Pack.INT_SIZE * 6);
    }

    /**
     * Set the size of the pack file header including the set of named static
     * pages and the journal headers.
     * 
     * @param addressPagePoolSize
     *            The size of the file header.
     */    
    public void setAddressPagePoolSize(int addressPagePoolSize)
    {
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE * 6, addressPagePoolSize);
        invalidate(Pack.LONG_SIZE + Pack.INT_SIZE * 6, Pack.INT_SIZE);
    }

    /**
     * Get the user boundary of the pack file. The user boundary is the 
     * position of the first user page, less the file header offset.
     * 
     * @return The position of the first user page.
     */
    public long getUserBoundary()
    {
        return bytes.getLong(Pack.LONG_SIZE + Pack.INT_SIZE * 7);
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
        bytes.putLong(Pack.LONG_SIZE + Pack.INT_SIZE * 7, userBoundary);
        invalidate(Pack.LONG_SIZE + Pack.INT_SIZE * 7, Pack.LONG_SIZE);
    }

    // TODO Comment.
    public long getEndOfSheaf()
    {
        return bytes.getLong(Pack.LONG_SIZE * 2 + Pack.INT_SIZE * 7);
    }

    // TODO Comment.
    public void setEndOfSheaf(long interimBoundary)
    {
        bytes.putLong(Pack.LONG_SIZE * 2 + Pack.INT_SIZE * 7, interimBoundary);
        invalidate(Pack.LONG_SIZE * 2 + Pack.INT_SIZE * 7, Pack.LONG_SIZE);
    }

    /**
     * Get the file position of the first temporary block node in a linked list
     * of temporary block nodes.
     * 
     * @return The position of the first temporary block node.
     */
    public long getFirstTemporaryNode()
    {
        return bytes.getLong(Pack.LONG_SIZE * 3 + Pack.INT_SIZE * 7);
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
        bytes.putLong(Pack.LONG_SIZE * 3 + Pack.INT_SIZE * 7, temporaries);
        invalidate(Pack.LONG_SIZE * 3 + Pack.INT_SIZE * 7, Pack.LONG_SIZE);
    }
    
    // TODO Comment.
    public long getByRemainingTable()
    {
        return bytes.getLong(Pack.LONG_SIZE * 4 + Pack.INT_SIZE * 7);
    }
    
    // TODO Comment.
    public void setByRemainingTable(long vacuumNode)
    {
        bytes.putLong(Pack.LONG_SIZE * 4 + Pack.INT_SIZE * 7, vacuumNode);
        invalidate(Pack.LONG_SIZE * 4 + Pack.INT_SIZE * 7, Pack.LONG_SIZE);
    }
}