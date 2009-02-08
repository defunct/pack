package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;


class NullJournalWriter
extends JournalWriter
{
    public NullJournalWriter(Sheaf sheaf, InterimPagePool interimPagePool, MoveNodeRecorder moveNodeRecorder, PageMoveTracker pageRecorder, DirtyPageSet dirtyPages)
    {
        super(sheaf, interimPagePool, moveNodeRecorder, pageRecorder, null, null, dirtyPages);
    }

    /**
     * A write operation that always returns false to force the creation of a
     * new linked list of journal pages.
     * 
     * @return False to force the call to {@link extend} to create a new liked
     *         list of journal pages.
     */
    public boolean write(Operation operation)
    {
        return false;
    }

    /**
     * Create a new journal writer that will write to the first page of a linked
     * list of journal pages.
     * 
     * @return A new linked list of journal pages.
     */
    public JournalWriter extend()
    {
        JournalPage journal = interimPagePool.newInterimPage(sheaf, JournalPage.class, new JournalPage(), dirtyPages);
        Movable start = new Movable(moveNodeRecorder.getMoveNode(), journal.getJournalPosition(), 0);
        pageRecorder.getJournalPages().add(journal.getRawPage().getPosition());
        return new JournalWriter(sheaf, interimPagePool, moveNodeRecorder, pageRecorder, journal, start, dirtyPages);
    }
}