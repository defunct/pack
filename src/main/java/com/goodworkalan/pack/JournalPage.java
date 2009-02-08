package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import com.goodworkalan.sheaf.DirtyPageSet;

/**
 * A page that stores journal entries in the interim region of the pack.
 * 
 * @author Alan Gutierrez
 */
final class JournalPage
extends RelocatablePage
{
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

        getRawPage().invalidate(0, Pack.JOURNAL_PAGE_HEADER_SIZE);
        dirtyPages.add(getRawPage());
        
        this.offset = Pack.JOURNAL_PAGE_HEADER_SIZE;
    }

    /** Load a journal page from the underlying raw page. */
    public void load()
    {
        this.offset = Pack.JOURNAL_PAGE_HEADER_SIZE;
    }
    
    /**
     * Checksum the entire journal page. In order to checksum only journal
     * pages I'll have to keep track of where the journal ends.
     * 
     * @param checksum The checksum algorithm.
     */
    public void checksum(Checksum checksum)
    {
        checksum.reset();
        ByteBuffer bytes = getRawPage().getByteBuffer();
        bytes.position(Pack.CHECKSUM_SIZE);
        while (bytes.position() != offset)
        {
            checksum.update(bytes.get());
        }
        bytes.putLong(0, checksum.getValue());
        getRawPage().invalidate(0, Pack.CHECKSUM_SIZE);
    }
    
    private ByteBuffer getByteBuffer()
    {
        ByteBuffer bytes = getRawPage().getByteBuffer();
        
        bytes.clear();
        bytes.position(offset);

        return bytes;
    }

    public boolean write(Operation operation, int overhead, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getByteBuffer();

            if (operation.length() + overhead < bytes.remaining())
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
     * <p>
     * FIXME Left off here.
     * 
     * @param position
     *            The file position to move to.
     */
    public void seek(long position)
    {
        synchronized (getRawPage())
        {
            this.offset = (int) (position - getRawPage().getPosition());
        }
    }

    /**
     * Create a blank operation of a type corresponding to the given type flag.
     * The blank operation is then loaded from the journal page at the current
     * offset into the journal page.
     * 
     * @param type
     *            The type of operation.
     * @return An operation corresponding to the given type flag.
     */
    private Operation newOperation(short type)
    {
        switch (type)
        {
            case Pack.ADD_VACUUM:
                return new Vacuum();
            case Pack.VACUUM:
                return new VacuumCheckpoint();
            case Pack.ADD_MOVE: 
                return new AddMove();
            case Pack.SHIFT_MOVE:
                return new ShiftMove();
            case Pack.CREATE_ADDRESS_PAGE:
                return new CreateAddressPage();
            case Pack.WRITE:
                return new Write();
            case Pack.FREE:
                return new Free();
            case Pack.NEXT_PAGE:
                return new NextOperation();
            case Pack.COPY:
                return new Copy();
            case Pack.TERMINATE:
                return new Terminate();
            case Pack.TEMPORARY:
                return new Temporary();
        }
        throw new IllegalStateException("Invalid type: " + type);
    }
    
    public Operation next()
    {
        ByteBuffer bytes = getByteBuffer();

        Operation operation = newOperation(bytes.getShort());
        operation.read(bytes);
        
        offset = bytes.position();
        
        return operation;
    }
}