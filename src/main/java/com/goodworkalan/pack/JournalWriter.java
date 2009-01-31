package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;


class JournalWriter
{
    protected final JournalPage journal;

    protected final Sheaf pager;
    
    protected final InterimPagePool interimPagePool;
    
    protected final DirtyPageSet dirtyPages;
    
    protected final PageRecorder pageRecorder;
    
    protected final Movable start;
    
    protected final MoveNodeRecorder moveNodeRecorder;
    
    public JournalWriter(Sheaf pager, InterimPagePool interimPagePool, MoveNodeRecorder moveNodeRecorder, PageRecorder pageRecorder, JournalPage journal, Movable start, DirtyPageSet dirtyPages)
    {
        this.pager = pager;
        this.interimPagePool = interimPagePool;
        this.pageRecorder = pageRecorder;
        this.journal = journal;
        this.start = start;
        this.dirtyPages = dirtyPages;
        this.moveNodeRecorder = moveNodeRecorder;
    }
    
    public Movable getJournalStart()
    {
        return start;
    }

    public long getJournalPosition()
    {
        return journal.getJournalPosition();
    }
    
    public boolean write(Operation operation)
    {
        return journal.write(operation, Pack.NEXT_PAGE_SIZE, dirtyPages);
    }
    
    public JournalWriter extend()
    {
        JournalPage nextJournal = interimPagePool.newInterimPage(pager, JournalPage.class, new JournalPage(), dirtyPages);
        journal.write(new NextOperation(nextJournal.getJournalPosition()), 0, dirtyPages);
        pageRecorder.getJournalPages().add(journal.getRawPage().getPosition());
        return new JournalWriter(pager, interimPagePool, moveNodeRecorder, pageRecorder, nextJournal, start, dirtyPages);
    }
    
    public JournalWriter reset()
    {
        return new NullJournalWriter(pager, interimPagePool, moveNodeRecorder, pageRecorder, dirtyPages);
    }
}