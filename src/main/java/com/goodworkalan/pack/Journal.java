package com.goodworkalan.pack;

import java.util.Set;
import java.util.zip.Adler32;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

// TODO Comment.
class Journal
{
    // TODO Comment.
    private JournalWriter writer;
    
    // TODO Comment.
    public Journal(Sheaf sheaf, InterimPagePool interimPagePool, DirtyPageSet dirtyPages)
    {
        writer = new NullJournalWriter(sheaf, interimPagePool, new Adler32(), dirtyPages);
    }
    
    // TODO Comment.
    public long getJournalStart()
    {
        if (writer.getJournalStart() == 0L)
        {
            writer = writer.extend();
        }
        return writer.getJournalStart();
    }
    
    // TODO Comment.
    public long getJournalPosition()
    {
        return writer.getJournalPosition();
    }

    // TODO Comment.
    public Set<Long> getJournalPages()
    {
        return writer.getJournalPages();
    }

    // TODO Comment.
    public void write(Operation operation)
    {
        while (!writer.write(operation))
        {
            writer = writer.extend();
        }
    }
    
    // TODO Comment.
    public void reset()
    {
        writer = writer.reset();
    }
}