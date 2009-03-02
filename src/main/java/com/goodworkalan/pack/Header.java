package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyMap;

/**
 * Reads and writes the header fields of a pack file. The header is built on top
 * of {@link DirtyMap} so that writing header fields will only cause the
 * dirty fields to be written when the header is flushed.
 * <p>
 * FIXME Need to lock on the header.
 * 
 * @author Alan Gutierrez
 */
final class Header extends DirtyMap
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

    /**
     * Return the underlying byte content of the header for writing.
     * 
     * @return The underlying byte content of the header for writing.
     */
    public ByteBuffer getByteBuffer()
    {
        return bytes;
    }

    /**
     * Get the position where the static block map of URIs to static address is
     * stored.
     * 
     * @return The position where the static block map is stored.
     */
    public long getStaticBlockMapStart()
    {
        return Pack.FILE_HEADER_SIZE + getJournalCount() * Pack.LONG_SIZE;
    }

    /**
     * Get the signature at the very start of the file that indicates that this
     * might be a <code>Pack</code> file.
     * 
     * @return The file signature.
     */
    public long getSignature()
    {
        return bytes.getLong(0);
    }

    /**
     * Get the signature at the very start of the file that indicates that this
     * might be a <code>Pack</code> file.
     * 
     * @param signature
     *            The file signature.
     */
    public void setSignature(long signature)
    {
        bytes.putLong(0, signature);
        dirty(0, Pack.LONG_SIZE);
    }

    /**
     * Get the flag indicating a soft shutdown or a hard shutdown. The value of
     * this flag is one of {@link Pack#SOFT_SHUTDOWN} or
     * {@link Pack#HARD_SHUTDOWN}.
     * 
     * @return The shutdown flag.
     */
    public int getShutdown()
    {
        return bytes.getInt(Pack.LONG_SIZE);
    }

    /**
     * Get the flag indicating a soft shutdown or a hard shutdown. The value of
     * this flag is one of {@link Pack#SOFT_SHUTDOWN} or
     * {@link Pack#HARD_SHUTDOWN}.
     * 
     * @param shutdown
     *            The shutdown flag.
     */
    public void setShutdown(int shutdown)
    {
        bytes.putInt(Pack.LONG_SIZE, shutdown);
        dirty(Pack.LONG_SIZE, Pack.INT_SIZE);
    }

    /**
     * Get the size of a page in the file.
     * 
     * @return The page size.
     */
    public int getPageSize()
    {
        return bytes.getInt(Pack.LONG_SIZE + Pack.INT_SIZE);
    }

    /**
     * Set the size of a page in the file.
     * 
     * @param pageSize
     *            The page size.
     */
    public void setPageSize(int pageSize)
    {
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE, pageSize);
        dirty(Pack.LONG_SIZE + Pack.INT_SIZE, Pack.INT_SIZE);
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
     * @param alignment
     *            The block alignment.
     */
    public void setAlignment(int alignment)
    {
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE * 2, alignment);
        dirty(Pack.LONG_SIZE + Pack.INT_SIZE * 2, Pack.INT_SIZE);
    }

    /**
     * Get the count of journal headers in the header of the file.
     * 
     * @return The journal count.
     */
    public int getJournalCount()
    {
        return bytes.getInt(Pack.LONG_SIZE + Pack.INT_SIZE * 3);
    }

    /**
     * Set the count of journal headers in the header of the file.
     * 
     * @param journalCount
     *            The journal count.
     */
    public void setJournalCount(int journalCount)
    {
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE * 3, journalCount);
        dirty(Pack.LONG_SIZE + Pack.INT_SIZE * 3, Pack.INT_SIZE);
    }

    /**
     * Get the count of static blocks.
     * 
     * @return The static block count.
     */
    public int getStaticBlockCount()
    {
        return bytes.getInt(Pack.LONG_SIZE + Pack.INT_SIZE * 4);
    }

    /**
     * Set the count of static blocks.
     * 
     * @param staticPageCount
     *            The static block count.
     */
    public void setStaticBlockCount(int staticPageCount)
    {
        bytes.putInt(Pack.LONG_SIZE + Pack.INT_SIZE * 4, staticPageCount);
        dirty(Pack.LONG_SIZE + Pack.INT_SIZE * 4, Pack.INT_SIZE);
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
        dirty(Pack.LONG_SIZE + Pack.INT_SIZE * 5, Pack.INT_SIZE);
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
        dirty(Pack.LONG_SIZE + Pack.INT_SIZE * 6, Pack.INT_SIZE);
    }

    /**
     * Get the user boundary of the pack file. The user boundary is the position
     * of the first user page, less the file header offset.
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
        dirty(Pack.LONG_SIZE + Pack.INT_SIZE * 7, Pack.LONG_SIZE);
    }

    /**
     * Get the address of the lookup page pool used to track address pages with
     * at least one address remaining for allocation.
     * 
     * @return The position of the address lookup page pool.
     */
    public long getAddressLookupPagePool()
    {
        return bytes.getLong(Pack.LONG_SIZE * 2 + Pack.INT_SIZE * 7);
    }

    /**
     * Set the address of the lookup page pool used to track address pages with
     * at least one address remaining for allocation.
     * 
     * @param position
     *            The position of the address lookup page pool.
     */
    public void setAddressLookupPagePool(long position)
    {
        bytes.putLong(Pack.LONG_SIZE * 2 + Pack.INT_SIZE * 7, position);
        dirty(Pack.LONG_SIZE * 2 + Pack.INT_SIZE * 7, Pack.LONG_SIZE);
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
        dirty(Pack.LONG_SIZE * 3 + Pack.INT_SIZE * 7, Pack.LONG_SIZE);
    }

    /**
     * Get the page position of the table of block pages ordered by bytes
     * available for block allocation.
     * 
     * @return The by remaining table address.
     */
    public long getByRemainingTable()
    {
        return bytes.getLong(Pack.LONG_SIZE * 4 + Pack.INT_SIZE * 7);
    }

    /**
     * Set the page position of the table of block pages ordered by bytes
     * available for block allocation.
     * 
     * @param byRemainingTable
     *            The by remaining table address.
     */
    public void setByRemainingTable(long byRemainingTable)
    {
        bytes.putLong(Pack.LONG_SIZE * 4 + Pack.INT_SIZE * 7, byRemainingTable);
        dirty(Pack.LONG_SIZE * 4 + Pack.INT_SIZE * 7, Pack.LONG_SIZE);
    }
}