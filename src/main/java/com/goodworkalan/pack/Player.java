package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Segment;


final class Player
{
    private final Bouquet bouquet;

    private final Segment header;

    private long entryPosition;

    private final DirtyPageSet dirtyPages;
    
    private final Journal vacuumJournal;
    
    private final SortedSet<Long> addresses;
    
    private final Set<Long> temporaryAddresses;
    
    public Player(Bouquet bouquet, Journal vacuumJournal, Segment header, DirtyPageSet dirtyPages)
    {
        ByteBuffer bytes = header.getByteBuffer();
        
        bytes.clear();
        
        this.bouquet = bouquet;
        this.header = header;
        this.entryPosition = bytes.getLong();
        this.dirtyPages = dirtyPages;
        this.addresses = new TreeSet<Long>();
        this.vacuumJournal = vacuumJournal;
        this.temporaryAddresses = new HashSet<Long>();
    }
    
    public Bouquet getBouquet()
    {
        return bouquet;
    }
    
    public Segment getJournalHeader()
    {
        return header;
    }
    
    public Journal getVacuumJournal()
    {
        return vacuumJournal;
    }

    public DirtyPageSet getDirtyPages()
    {
        return dirtyPages;
    }
    
    public SortedSet<Long> getAddresses()
    {
        return addresses;
    }
    
    public Set<Long> getTemporaryAddresses()
    {
        return temporaryAddresses;
    }
    
    private void execute()
    {
        JournalPage journalPage = bouquet.getSheaf().getPage(entryPosition, JournalPage.class, new JournalPage());
        
        journalPage.seek(entryPosition);
        
        Operation operation = journalPage.next(); 
        while (!operation.terminate())
        {
            operation.commit(this);
            journalPage = operation.getJournalPage(this, journalPage);
            operation = journalPage.next();
        }

        entryPosition = journalPage.getJournalPosition();
    }

    public void vacuum()
    {
        execute();
    }

    public void commit()
    {
        execute();

        header.getByteBuffer().clear();
        header.getByteBuffer().putLong(0, 0L);

        dirtyPages.flush();
        header.write(bouquet.getSheaf().getFileChannel());
        try
        {
            bouquet.getSheaf().getFileChannel().force(true);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_FORCE, e);
        }
        
        bouquet.getJournalHeaders().free(header);
    }
}