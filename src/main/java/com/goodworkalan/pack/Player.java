package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.goodworkalan.region.Region;
import com.goodworkalan.sheaf.DirtyPageSet;

/**
 * Plays a journal, calling the {@link Operation#execute(Player) commit} method
 * of each journal operation and freeing the journal pages after commit.
 * <p>
 * Journals are used to allocate new address pages as well as commit changes
 * made by mutations. The player is also a bouquet pattern, exposing most of its
 * state to the journal operations.
 * 
 * @author Alan Gutierrez
 */
final class Player
{
    /** The bouquet of services. */
    private final Bouquet bouquet;

    /** The journal header that indicates the first journal operation. */
    private final Region journalHeader;

    /** The dirty page set used to track dirty pages during playback. */
    private final DirtyPageSet dirtyPages;
    
    /** The set of freed addresses that have been locked. */
    private final SortedSet<Long> lockedAddresses;
    
    /** The set of freed temporary references that have been locked. */
    private final Set<Long> lockedTemporaryReferences;
    
    /**
     * The block pages that have had one or more blocks freed during playback.
     */
    private final Set<Long> freedBlockPages;

    /**
     * The block pages that contained newly allocated blocks assigned to
     * addresses during playback.
     */
    private final Set<Long> allocatedBlockPages;
    
    /**
     * The set of pages to return to the free interim page pool after the player
     * commits.
     */
    private final Set<Long> interimPages;

    /** The journal pages visited during playback. */
    private final Set<Long> journalPages;

    /**
     * Create a recovery journal player that will play the the journal
     * operations starting by the start of the journal referenced by the the
     * journal header.
     * 
     * @param bouquet
     *            The bouquet of services.
     * @param journalHeader
     *            The journal header.
     * @param dirtyPages
     *            The dirty page set.
     */
    public Player(Bouquet bouquet, Region journalHeader, DirtyPageSet dirtyPages)
    {
        this.bouquet = bouquet;
        this.journalHeader = journalHeader;
        this.dirtyPages = dirtyPages;
        this.lockedAddresses = new TreeSet<Long>();
        this.lockedTemporaryReferences = new HashSet<Long>();
        this.journalPages = new HashSet<Long>();
        this.freedBlockPages = new HashSet<Long>();
        this.interimPages = new HashSet<Long>();
        this.allocatedBlockPages = new HashSet<Long>();
    }

    /**
     * Create a player that will assign the first operation of the given journal
     * to a journal header and play the the journal operations in the journal.
     * <p>
     * If journal playback fails due to a hard shutdown, the journal can be
     * found via the journal header and replayed.
     * 
     * @param bouquet
     *            The bouquet of services.
     * @param journal
     *            The journal.
     * @param dirtyPages
     *            The dirty page set.
     */
    public Player(Bouquet bouquet, Journal journal, DirtyPageSet dirtyPages)
    {
        this(bouquet, allocateHeader(journal, bouquet, dirtyPages), dirtyPages);
    }

    /**
     * Allocate a journal header from the pool of journal headers and assign the
     * first operation of the given journal to the journal header.
     * 
     * @param journal
     *            The journal.
     * @param bouquet
     *            The bouquet of services.
     * @param dirtyPages
     *            The dirty page set.
     * @return A journal header that refrerences the first operation of the
     *         journal.
     */
    private static Region allocateHeader(Journal journal, Bouquet bouquet, DirtyPageSet dirtyPages)
    {
        Region header = bouquet.getJournalHeaders().allocate();
        header.getByteBuffer().putLong(bouquet.getAddressBoundary().adjust(journal.getJournalStart()));
        
        // Write and force our journal.
        dirtyPages.flush();
        try
        {
            ByteBuffer byteBuffer = header.getByteBuffer();
            byteBuffer.clear();
            bouquet.getSheaf().getFileChannel().write(byteBuffer);
        }
        catch (IOException e)
        {
            throw new PackException(PackException.ERROR_IO_WRITE, e);
        }
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
    
    /**
     * Get the bouquet of services.
     * 
     * @return The bouquet of services.
     */
    public Bouquet getBouquet()
    {
        return bouquet;
    }
    
    /**
     * Get the journal header that indicates the first journal operation.
     * 
     * @return The journal header.
     */
    public Region getJournalHeader()
    {
        return journalHeader;
    }

    /**
     * Get the dirty page set used to track dirty pages during playback.
     * 
     * @return The dirty page set.
     */
    public DirtyPageSet getDirtyPages()
    {
        return dirtyPages;
    }

    /**
     * Get the set of freed addresses that have been locked.
     * 
     * @return The set of freed addresses that have been locked.
     */
    public SortedSet<Long> getLockedAddresses()
    {
        return lockedAddresses;
    }

    /**
     * Get the set of freed temporary references that have been locked.
     * 
     * @return The set of freed temporary references that have been locked.
     */
    public Set<Long> getLockedTemporaryReferences()
    {
        return lockedTemporaryReferences;
    }

    /**
     * Get the block pages that have had one or more blocks freed during
     * playback.
     * 
     * @return The block pages that have had one or more blocks freed during
     *         playback.
     */
    public Set<Long> getFreedBlockPages()
    {
        return freedBlockPages;
    }

    /**
     * Get the block pages that contained newly allocated blocks assigned to
     * addresses during playback.
     * 
     * @return The block pages that contained newly allocated blocks assigned to
     *         addresses during playback.
     */
    public Set<Long> getAllocatedBlockPages()
    {
        return allocatedBlockPages;
    }

    /**
     * Get the set of pages to return to the free interim page pool after the
     * player commits.
     * 
     * @return The set of pages to return to the free interim page pool after
     *         the player commits.
     */
    public Set<Long> getInterimPages()
    {
        return interimPages;
    }

    /**
     * Execute the journal operations starting by the start of the journal
     * referenced by the the journal header.
     * <p>
     * The journal operations must include a commit or checkpoint operations
     * followed by a terminate operation at the end of the journal.
     */
    public void play()
    {
        long operationPosition = journalHeader.getByteBuffer().getLong(0);
        JournalPage journalPage = bouquet.getSheaf().getPage(operationPosition, JournalPage.class, new JournalPage());
        
        journalPage.seek(operationPosition);
        
        Operation operation = journalPage.next(); 
        while (!operation.terminate())
        {
            operation.execute(this);
            journalPage = operation.getJournalPage(this, journalPage);
            journalPages.add(journalPage.getRawPage().getPosition());
            operation = journalPage.next();
        }

        bouquet.getJournalHeaders().free(journalHeader);

        // Unlock any addresses that were returned as free to their
        // address pages, but were locked to prevent the commit of a
        // reallocation until this commit completed.
        for (long address : getLockedAddresses())
        {
            bouquet.getAddressLocker().unlatch(address);
        }
        bouquet.getTemporaryPool().unlock(getLockedTemporaryReferences());
        
        
        for(long position : bouquet.getAddressBoundary().adjust(journalPages))
        {
            bouquet.getInterimPagePool().free(position);
        }
        
        bouquet.getUserPagePool().add(getFreedBlockPages(), getAllocatedBlockPages());
    }
}