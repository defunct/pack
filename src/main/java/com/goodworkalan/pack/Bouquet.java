package com.goodworkalan.pack;

import java.net.URI;
import java.util.Map;

import com.goodworkalan.lock.many.LatchSet;
import com.goodworkalan.region.Header;
import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Gathers the various services so that they are in one place, but not all in
 * one class.
 * <p>
 * These services depend on each other, and many could be placed in a single
 * class, but the bouquet pattern allows us some modularity. Where one service
 * depends on another, that service is given as a parameter, rather than having
 * the services combined.
 * 
 * @author Alan Gutierrez
 */
final class Bouquet
{
    /** The pack object. */
    private final Pack pack;
    
    /** Housekeeping information stored at the head of the file. */
    private final Header<Integer> header;

    /**
     * A map of URIs that identify the addresses of static blocks specified
     * at the creation of the pack.
     */
    private final Map<URI, Long> staticBlocks;

    /** The block alignment. */
    private final int alignment;

    /**
     * The set of journal header positions stored in a {@link PositionSet} that
     * tracks the availability of a fixed number of header position reference
     * positions and blocks a thread that requests a position when the set is
     * empty.
     */
    private final PositionSet journalHeaders;

    /** The page manager. */
    private final Sheaf sheaf;
    
    /**
     * A synchronization strategy that prevents addresses that have been freed
     * from overwriting reallocations.
     */
    private final LatchSet<Long> addressLocker;
    
    /** The address page pool. */
    private final AddressPagePool addressPagePool;

    /** The boundary between address pages and user data pages. */
    private final AddressBoundary addressBoundary;

    /** The user page pool. */
    private final UserPagePool userPagePool;

    /** The temporary address reference pool. */
    private final TemporaryPool temporaryPool;
    
    /** The interim page pool. */
    private final InterimPagePool interimPagePool;
    
    /**
     * A mutex to ensure that only one thread at a time is vacuuming the pack.
     */
    private final Object vacuumMutex;

    /**
     * The dirty page set used by the by remaining table for all user pages and
     * by the journaled file vacuum.
     */
    private final DirtyPageSet vacuumDirtyPages;
    
    /**
     * Create a bouquet of the given services.
     * 
     * @param header
     *            Housekeeping information stored at the head of the file.
     * @param staticBlocks
     *            A map of URIs that identify the addresses of static blocks
     *            specified at the creation of the pack.
     * @param userBoundary
     *            The boundary between address pages and user data pages.
     * @param sheaf
     *            Page management.
     * @param addressPagePool
     *            The address page pool
     * @param interimPagePool
     *            The interim page pool
     * @param temporaryFactory
     *            The temporary address reference pool.
     */
    public Bouquet(Header<Integer> header, Map<URI, Long> staticBlocks, AddressBoundary userBoundary, Sheaf sheaf, AddressPagePool addressPagePool, InterimPagePool interimPagePool, TemporaryPool temporaryFactory)
    {
        this.alignment = header.get(Housekeeping.ALIGNMENT).getByteBuffer().getInt(0);
        this.pack = new Pack(this);
        this.header = header;
        this.staticBlocks = staticBlocks;
        this.journalHeaders = new PositionSet(Pack.FILE_HEADER_SIZE, header.get(Housekeeping.JOURNAL_COUNT).getByteBuffer().getInt(0));
        this.addressBoundary = userBoundary;
        this.sheaf = sheaf;
        this.addressPagePool = addressPagePool;
        this.vacuumDirtyPages = new DirtyPageSet();
        this.interimPagePool = interimPagePool;
        this.userPagePool = new UserPagePool(new ByRemainingTable(sheaf, userBoundary, interimPagePool, alignment, pack.getMaximumBlockSize(), vacuumDirtyPages), sheaf.getPageSize(), alignment);
        this.temporaryPool = temporaryFactory;
        this.vacuumMutex = new Object();
        this.addressLocker = new LatchSet<Long>(64);
    }

    /**
     * Get the pack associated with this bouquet of services.
     * 
     * @return The pack.
     */
    public Pack getPack()
    {
        return pack;
    }

    /**
     * Get the housekeeping information stored at the head of the file.
     * 
     * @return The housekeeping information stored at the head of the file.
     */
    public Header<Integer> getHeader()
    {
        return header;
    }
    
    /**
     * Return a map of named pages that maps a URI to the address of a static
     * page.
     * 
     * @return The map of named static pages.
     * 
     * @see com.goodworkalan.pack.Pack#getStaticBlocks()
     */
    public Map<URI, Long> getStaticBlocks()
    {
        return staticBlocks;
    }

    /**
     * Get the alignment to which all block allocations are rounded.
     * 
     * @return The alignment to which all block allocations are rounded.
     */
    public int getAlignment()
    {
        return alignment;
    }

    /**
     * Get the set of position headers used to reserve journal headers.
     * 
     * @return The set of position headers.
     */
    public PositionSet getJournalHeaders()
    {
        return journalHeaders;
    }

    /**
     * Get the page manager.
     * 
     * @return The page manager.
     */
    public Sheaf getSheaf()
    {
        return sheaf;
    }

    /**
     * Returns the address locker which will block a reallocation from
     * committing until the commit that freed an address has completed. This
     * prevents the reallocation from being overwritten by playback of the
     * journaled free.
     * 
     * @return The per pack address locker.
     */
    public LatchSet<Long> getAddressLocker()
    {
        return addressLocker;
    }

    /**
     * Get the address page pool.
     * 
     * @return The address page pool.
     */
    public AddressPagePool getAddressPagePool()
    {
        return addressPagePool;
    }

    /**
     * Get the boundary between address pages and user data pages.
     *
     * @return The boundary between address pages and user data pages.
     */
    public AddressBoundary getAddressBoundary()
    {
        return addressBoundary;
    }

    /**
     * Get the user page pool.
     * 
     * @return The user page pool.
     */
    public UserPagePool getUserPagePool()
    {
        return userPagePool;
    }

    /**
     * Get the temporary address reference pool.
     * 
     * @return The temporary address reference pool.
     */
    public TemporaryPool getTemporaryPool()
    {
        return temporaryPool;
    }

    /**
     * Get the interim page pool.
     * 
     * @return The interim page pool.
     */
    public InterimPagePool getInterimPagePool()
    {
        return interimPagePool;
    }
    
    /**
     * Return the per pack mutex used to ensure that only one thread at a time
     * is vacuuming the pack.
     * 
     * @return The vacuum expand mutex.
     */
    public Object getVacuumMutex()
    {
        return vacuumMutex;
    }
}
