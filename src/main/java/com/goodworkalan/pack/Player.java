package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.Adler32;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Segment;


final class Player
{
    private final Bouquet bouquet;

    private final Segment header;

    private long entryPosition;

    private final DirtyPageSet dirtyPages;
    
    private final SortedSet<Long> setOfAddresses;
    
    private final Set<Vacuum> setOfVacuums; 
    
    private final LinkedList<Move> listOfMoves;
    
    private final Adler32 adler32;
    
    public Player(Bouquet bouquet, Segment header, DirtyPageSet dirtyPages)
    {
        ByteBuffer bytes = header.getByteBuffer();
        
        bytes.clear();
        
        this.bouquet = bouquet;
        this.header = header;
        this.entryPosition = bytes.getLong();
        this.dirtyPages = dirtyPages;
        this.setOfAddresses = new TreeSet<Long>();
        this.setOfVacuums = new HashSet<Vacuum>();
        this.listOfMoves = new LinkedList<Move>();
        this.adler32 = new Adler32();
    }
    
    public Bouquet getBouquet()
    {
        return bouquet;
    }
    
    public Adler32 getAdler32()
    {
        return adler32;
    }
    
    public Segment getJournalHeader()
    {
        return header;
    }

    public DirtyPageSet getDirtyPages()
    {
        return dirtyPages;
    }
    
    public SortedSet<Long> getAddressSet()
    {
        return setOfAddresses;
    }
    
    public Set<Vacuum> getVacuumSet()
    {
        return setOfVacuums;
    }
    
    public LinkedList<Move> getMoveList()
    {
        return listOfMoves;
    }
    
    public long adjust(long position)
    {
        return bouquet.adjust(getMoveList(), position);
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