package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.goodworkalan.sheaf.DirtyPageSet;

// FIXME Comment.
final class Player
{
    private final Bouquet bouquet;

    private final JournalHeader header;

    private long entryPosition;

    private final DirtyPageSet dirtyPages;
    
    private final SortedSet<Long> addresses;
    
    private final Set<Long> temporaryAddresses;
    
    private final Set<Long> journalPages;
    
    private final Set<Long> freedBlockPages;
    
    private final Set<Long> allocatedBlockPages;
    
    public Player(Bouquet bouquet, JournalHeader header, DirtyPageSet dirtyPages)
    {
        ByteBuffer bytes = header.getByteBuffer();
        
        bytes.clear();
        
        this.bouquet = bouquet;
        this.header = header;
        this.entryPosition = bytes.getLong();
        this.dirtyPages = dirtyPages;
        this.addresses = new TreeSet<Long>();
        this.temporaryAddresses = new HashSet<Long>();
        this.journalPages = new HashSet<Long>();
        this.freedBlockPages = new HashSet<Long>();
        this.allocatedBlockPages = new HashSet<Long>();
    }
    
    public Player(Bouquet bouquet, Journal journal, DirtyPageSet dirtyPages)
    {
        this(bouquet, allocateHeader(journal, bouquet, dirtyPages), dirtyPages);
    }
    
    private static JournalHeader allocateHeader(Journal journal, Bouquet bouquet, DirtyPageSet dirtyPages)
    {
        JournalHeader header = bouquet.getJournalHeaders().allocate();
        header.getByteBuffer().putLong(bouquet.getUserBoundary().adjust(bouquet.getSheaf(), journal.getJournalStart()));
        
        // Write and force our journal.
        dirtyPages.flush();
        header.write(bouquet.getSheaf().getFileChannel());
        try
        {
            bouquet.getSheaf().getFileChannel().force(true);
        }
        catch (IOException e)
        {
            throw new PackException(PackException.ERROR_IO_FORCE, e);
        }
        
        return header;
    }
    
    public Bouquet getBouquet()
    {
        return bouquet;
    }
    
    public JournalHeader getJournalHeader()
    {
        return header;
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
    
    public Set<Long> getFreedBlockPages()
    {
        return freedBlockPages;
    }
    
    public Set<Long> getAllocatedBlockPages()
    {
        return allocatedBlockPages;
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
            journalPages.add(journalPage.getRawPage().getPosition());
            operation = journalPage.next();
        }

        entryPosition = journalPage.getJournalPosition();
    }

    public void commit()
    {
        execute();
        
        bouquet.getJournalHeaders().free(header);

        // Unlock any addresses that were returned as free to their
        // address pages, but were locked to prevent the commit of a
        // reallocation until this commit completed.
        bouquet.getAddressLocker().unlock(getAddresses());
        bouquet.getTemporaryPool().unlock(getTemporaryAddresses());
        
        
        for(long position : bouquet.getUserBoundary().adjust(bouquet.getSheaf(), journalPages))
        {
            bouquet.getSheaf().free(position);
            bouquet.getInterimPagePool().free(position);
        }
        
        bouquet.getUserPagePool().add(getFreedBlockPages(), getAllocatedBlockPages());
    }
}