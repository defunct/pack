package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

class Journal
{
    private JournalWriter writer;
    
    public Journal(Sheaf pager, InterimPagePool interimPagePool, MoveNodeRecorder moveNodeRecorder, PageRecorder pageRecorder, DirtyPageSet dirtyPages)
    {
        writer = new NullJournalWriter(pager, interimPagePool, moveNodeRecorder, pageRecorder, dirtyPages);
    }
    
    public Movable getJournalStart()
    {
        return writer.getJournalStart();
    }
    
    public long getJournalPosition()
    {
        return writer.getJournalPosition();
    }

    public void write(Operation operation)
    {
        while (!writer.write(operation))
        {
            writer = writer.extend();
        }
    }
    
    public void reset()
    {
        writer = writer.reset();
    }
}