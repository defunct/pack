package com.goodworkalan.pack;

import java.util.Set;
import java.util.zip.Checksum;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * A container for a last page in a linked list of journal pages.
 * <p>
 * When writing an operation, the journal page write method can be asked to
 * ensure that there is enough space for a next journal page operation. If there
 * is not enough space for the operation and the next page operation the write
 * method returns false. The journal writer will then allocate a new journal
 * page and write a next operation operation that will go to the start of the
 * new journal page.
 * 
 * @author Alan Gutierrez
 */
class JournalWriter
{
    /** Size of a next page operation. */ 
    private final static int NEXT_PAGE_SIZE = Pack.SHORT_SIZE + Pack.LONG_SIZE;

    /** The sheaf from which to allocate pages. */
    protected final Sheaf sheaf;
    
    /** The interim page pool from which to allocate pages. */
    protected final InterimPagePool interimPagePool;
    
    /** The set of page positions used by this journal. */
    protected final Set<Long> journalPages;
 
    /**
     * The position of the first journal operation on the first journal page.  
     */
    protected long start;

    /** The current journal page. */
    protected final JournalPage journal;

    /** The set of dirty pages. */
    protected final DirtyPageSet dirtyPages;
    
    /** Checksum to use to generate the checksum for the journal page. */
    protected final Checksum checksum;

    /**
     * Create a journal writer that will write to the given journal page and
     * allocate subsequent pages from the given sheaf and given interim page
     * pool.
     * <p>
     * The given move node recorder is used by reset to create a
     * {@link NullJournalWriter} which in turn uses it to create a movable
     * reference to the first page of the linked list of journal pages.
     * <p>
     * New page allocations are added to the given page move tracker.
     * 
     * @param sheaf
     *            The sheaf from which to allocate pages.
     * @param interimPagePool
     *            The interim page pool from which to allocate pages.
     * @param start
     *            The position of the first operation on the first page of the
     *            journal.
     * @param journalPage
     *            The current journal page.
     * @param journalPages
     *            The set of page positions used by this journal.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    public JournalWriter(Sheaf sheaf, InterimPagePool interimPagePool, Checksum checksum, long start, JournalPage journalPage, Set<Long> journalPages, DirtyPageSet dirtyPages)
    {
        this.sheaf = sheaf;
        this.interimPagePool = interimPagePool;
        this.journal = journalPage;
        this.journalPages = journalPages;
        this.checksum = checksum;
        this.start = start;
        this.dirtyPages = dirtyPages;
    }

    /**
     * Get the position of the first journal operation on the first journal
     * page. This position will not have been adjusted for any page moves.
     * 
     * @return The position of the first journal operation on the first journal
     *         page.
     */
    public long getJournalStart()
    {
        return start;
    }

    /**
     * Get the file position of the current offset into the current journal
     * page.
     * <p>
     * This method is used to determine the file position of the journal when
     * recording {@link NextOperation} jumps in the journal playback.
     * 
     * @return The file position of the journal page plus the offset into the
     *         current journal page.
     */
    public long getJournalPosition()
    {
        return journal.getJournalPosition();
    }

    /**
     * Get the set of page positions used by this journal.
     * 
     * @return The set of page positions used by this journal.
     */
    public Set<Long> getJournalPages()
    {
        return journalPages;
    }

    /**
     * Write the given operation to the current journal page if the operation
     * will fit on the journal page. The operation will fit if there is enough
     * space remaining for the given operation plus a next journal page
     * operation. If the operation will not fit, this method returns false. The
     * caller must then extend the linked list of journal pages by calling the
     * {@link #extend() extend} method of this journal writer.
     * 
     * @param operation
     *            The operation to write.
     * @return True if their is enough space remaining for the operation.
     */
    public boolean write(Operation operation)
    {
        return journal.write(operation, NEXT_PAGE_SIZE, dirtyPages);
    }

    /**
     * Create a new journal writer that will write to a journal page that has
     * been appended to the linked list of journal pages.
     * <p>
     * This method will allocate a new journal page and add it to the the page
     * tracker. It writes a next operation operation that references the first
     * operation of the newly created page as the last operation to the current
     * page. It then creates a new journal writer that writes to the newly
     * created journal page.
     * <p>
     * The movable reference to the first page in the link list of journal pages
     * is given to the newly created journal writer.
     * 
     * @return A new journal writer that will write to a journal page that has
     *         been appended to the linked list of journal pages.
     */
    public JournalWriter extend()
    {
        JournalPage nextJournal = interimPagePool.newInterimPage(sheaf, JournalPage.class, new JournalPage(), dirtyPages, false);
        journal.write(new NextOperation(nextJournal.getJournalPosition()), 0, dirtyPages);
        journal.writeChecksum(checksum);
        journalPages.add(nextJournal.getRawPage().getPosition());
        return new JournalWriter(sheaf, interimPagePool, checksum, start, nextJournal, journalPages, dirtyPages);
    }

    /**
     * Returns a null journal writer that does not reference a journal page used
     * to initiate a new linked list of journal pages.
     * 
     * @return A null journal writer.
     */
    public JournalWriter reset()
    {
        return new NullJournalWriter(sheaf, interimPagePool, checksum, dirtyPages);
    }
}