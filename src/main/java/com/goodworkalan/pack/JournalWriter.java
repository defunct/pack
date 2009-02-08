package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
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
    /** The sheaf from which to allocate pages. */
    protected final Sheaf sheaf;
    
    /** The interim page pool from which to allocate pages. */
    protected final InterimPagePool interimPagePool;
    
    /**
     * A move node recorder to obtain the move node necessary to create the
     * movable reference to the first page in the linked list of journal pages.
     */
    protected final MoveNodeMoveTracker moveNodeRecorder;

    /** New journal page allocations are reported to this page tracker. */
    protected final PageMoveTracker pageRecorder;
    
    /**
     * A movable reference to the first page in the linked list of journal
     * pages.
     */
    protected final Movable start;

    /** The current journal page. */
    protected final JournalPage journal;

    /** The set of dirty pages. */
    protected final DirtyPageSet dirtyPages;

    /**
     * Create a journal writer that will allocate pages from the given sheaf and
     * given interim page pool.
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
     * @param moveNodeRecorder
     *            A move node recorder to obtain the move node necessary to
     *            create the movable reference to the first page in the linked
     *            list of journal pages.
     * @param pageRecorder
     *            New journal page allocations are reported to this page
     *            tracker.
     * @param start
     *            A movable reference to the first page in the linked list of
     *            journal pages.
     * @param journal
     *            The current journal page.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    public JournalWriter(Sheaf pager, InterimPagePool interimPagePool, MoveNodeMoveTracker moveNodeRecorder, PageMoveTracker pageRecorder, Movable start, JournalPage journal, DirtyPageSet dirtyPages)
    {
        this.sheaf = pager;
        this.interimPagePool = interimPagePool;
        this.pageRecorder = pageRecorder;
        this.journal = journal;
        this.start = start;
        this.dirtyPages = dirtyPages;
        this.moveNodeRecorder = moveNodeRecorder;
    }

    /**
     * Get a movable reference to the first journal operation.
     * 
     * @return A movable reference to the first journal operation.
     */
    public Movable getJournalStart()
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
        return journal.write(operation, Pack.NEXT_PAGE_SIZE, dirtyPages);
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
        JournalPage nextJournal = interimPagePool.newInterimPage(sheaf, JournalPage.class, new JournalPage(), dirtyPages);
        journal.write(new NextOperation(nextJournal.getJournalPosition()), 0, dirtyPages);
        pageRecorder.getJournalPages().add(journal.getRawPage().getPosition());
        return new JournalWriter(sheaf, interimPagePool, moveNodeRecorder, pageRecorder, start, nextJournal, dirtyPages);
    }

    /**
     * Returns a null journal writer that does not reference a journal page used
     * to initiate a new linked list of journal pages.
     * 
     * @return A null journal writer.
     */
    public JournalWriter reset()
    {
        return new NullJournalWriter(sheaf, interimPagePool, moveNodeRecorder, pageRecorder, dirtyPages);
    }
}