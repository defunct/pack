package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;

/**
 * A page that stores journal entries in the interim region of the pack.
 * <p>
 * Journal pages are created through a {@link JournalWriter} which manages the
 * allocation of journal pages. The pages are read and the operations performed
 * by a {@link Player}.
 * <p>
 * The journal page has an offset into the page where operations are read and
 * written. The {@link Operation} class has a read and write method and writes
 * its fields to the journal page itself.
 * <p>
 * When writing an operation the operation will write out the type flag for the
 * operation followed by the operation fields. When reading an operation, the
 * journal page will read the type flag, create the operation class associated
 * with the type, then the newly created class will read the fields.
 * 
 * @author Alan Gutierrez
 */
final class JournalPage
extends Page
{
    /** Journal page header size, journal data length plus checksum. */
    final static int JOURNAL_PAGE_HEADER_SIZE = Pack.INT_SIZE + Pack.LONG_SIZE;
    
    /** The offset form which to read the next journal entry. */
    private int offset;

    /**
     * Create a new journal page on the underlying raw page.
     * 
     * @param dirtyPages The set of dirty pages.
     */
    public void create(DirtyPageSet dirtyPages)
    {
        ByteBuffer bytes = getRawPage().getByteBuffer();
        
        bytes.clear();
        bytes.putLong(0);
        bytes.putInt(0);

        getRawPage().invalidate(0, JOURNAL_PAGE_HEADER_SIZE);
        dirtyPages.add(getRawPage());
        
        this.offset = JOURNAL_PAGE_HEADER_SIZE;
    }

    /** Load a journal page from the underlying raw page. */
    public void load()
    {
        this.offset = JOURNAL_PAGE_HEADER_SIZE;
    }
    
    /**
     * Checksum the entire journal page. In order to checksum only journal
     * pages I'll have to keep track of where the journal ends.
     * 
     * @param checksum The checksum algorithm.
     */
    public void writeChecksum(Checksum checksum)
    {
        getRawPage().getByteBuffer().putLong(0, getChecksum(checksum));
        getRawPage().invalidate(0, Pack.LONG_SIZE);
    }
    
    // TODO Comment.
    private long getChecksum(Checksum checksum)
    {
        checksum.reset();
        ByteBuffer bytes = getRawPage().getByteBuffer();
        bytes.position(Pack.LONG_SIZE);
        while (bytes.position() != offset)
        {
            checksum.update(bytes.get());
        }
        return checksum.getValue();
    }
    
    // TODO Comment.
    public boolean isValidChecksum(Checksum checksum)
    {
        return getRawPage().getByteBuffer().getLong(0) == getChecksum(checksum);
    }

    /**
     * Get the underlying byte buffer for the raw page with the position set to
     * the offset.
     * 
     * @return The content at the offset.
     */
    private ByteBuffer getByteBuffer()
    {
        ByteBuffer bytes = getRawPage().getByteBuffer();
        
        bytes.clear();
        bytes.position(offset);

        return bytes;
    }

    /**
     * Write the given operation to the journal page ensuring that the page will
     * have the given remaining count of bytes available for a next journal page
     * operation.
     * <p>
     * The overhead bytes represent the length of a next journal page operation,
     * or zero if the operation is the final next journal page operation.
     * <p>
     * Returns true if the operation is written, false if the operation would
     * not fit and preserve the given remaining count of bytes.
     * 
     * @param operation
     *            The operation to write.
     * @param remaining
     *            The amount of space to reserve for next
     * @param dirtyPages
     *            The set of dirty pages.
     * @return True if the operation is written, false if the operation would
     *         not fit and preserve the given remaining count of bytes.
     */
    public boolean write(Operation operation, int remaining, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getByteBuffer();

            if (operation.length() + remaining < bytes.remaining())
            {
                getRawPage().invalidate(offset, operation.length());
                operation.write(bytes);
                offset = bytes.position();
                dirtyPages.add(getRawPage());
                return true;
            }
            
            return false;
        }
    }

    /**
     * Get the file position of the current offset into the journal page.
     * <p>
     * This method is used to determine the file position of the journal when
     * recording {@link NextOperation} jumps in the journal playback.
     * 
     * @return The file position of the journal page plus the offset into the
     *         journal page.
     */
    public long getJournalPosition()
    {
        synchronized (getRawPage())
        {
            return getRawPage().getPosition() + offset;
        }
    }

    /**
     * Set the offset to reference the offset indicated by the given file
     * position. The offset is determined by the difference between the given
     * file position and the file position of the underlying raw page.
     * 
     * @param position
     *            The file position plus offset to move to.
     */
    public void seek(long position)
    {
        synchronized (getRawPage())
        {
            this.offset = (int) (position - getRawPage().getPosition());
        }
    }

    /**
     * Read the next operation at of the offset from this page journal
     * operations.
     * <p>
     * The offset will be advanced by the length of the operation record.
     * 
     * @return The next operation.
     */
    public Operation next()
    {
        ByteBuffer bytes = getByteBuffer();

        Operation operation = Operation.newOperation(bytes.getShort());
        operation.read(bytes);
        
        offset = bytes.position();
        
        return operation;
    }
}