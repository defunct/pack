package com.goodworkalan.pack;

import java.util.Set;
import java.util.zip.Adler32;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

class Journal
{
    private JournalWriter writer;
    
    public Journal(Sheaf sheaf, InterimPagePool interimPagePool, DirtyPageSet dirtyPages)
    {
        writer = new NullJournalWriter(sheaf, interimPagePool, new Adler32(), dirtyPages);
    }
    
    public long getJournalStart()
    {
        if (writer.getJournalStart() == 0L)
        {
            writer = writer.extend();
        }
        return writer.getJournalStart();
    }
    
    public long getJournalPosition()
    {
        return writer.getJournalPosition();
    }

    public Set<Long> getJournalPages()
    {
        return writer.getJournalPages();
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