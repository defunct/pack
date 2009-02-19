package com.goodworkalan.pack;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private final Header header;

    /**
     * A map of URIs that identify the addresses of static blocks specified
     * at the creation of the pack.
     */
    private final Map<URI, Long> staticBlocks;

    /**
     * The set of journal header positions stored in a {@link PositionSet} that
     * tracks the availability of a fixed number of header position reference
     * positions and blocks a thread that requests a position when the set is
     * empty.
     */
    private final PositionSet journalHeaders;

    /** The page manager. */
    private final Sheaf sheaf;
    
    /** A read/write lock to guard the address page to user boundary. */
    private final ReadWriteLock pageMoveLock;

    /**
     * A synchronization strategy that prevents addresses that have been freed
     * from overwriting reallocations.
     */
    private final AddressLocker addressLocker;
    
    /** The address page pool. */
    private final AddressPagePool addressPagePool;

    /** The boundary between address pages and user data pages. */
    private final UserBoundary userBoundary;

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
     * @param temporaryFactory
     *            The temporary address reference pool.
     */
    public Bouquet(Header header, Map<URI, Long> staticBlocks, UserBoundary userBoundary, Sheaf sheaf, AddressPagePool addressPagePool, TemporaryPool temporaryFactory)
    {
        this.pack = new Pack(this);
        this.header = header;
        this.staticBlocks = staticBlocks;
        this.journalHeaders = new PositionSet(Pack.FILE_HEADER_SIZE, header.getInternalJournalCount());
        this.userBoundary = userBoundary;
        this.sheaf = sheaf;
        this.addressPagePool = addressPagePool;
        this.vacuumDirtyPages = new DirtyPageSet();
        this.userPagePool = new UserPagePool(new ByRemainingTable(this, vacuumDirtyPages), header.getPageSize(), header.getAlignment());
        this.interimPagePool = new InterimPagePool();
        this.temporaryPool = temporaryFactory;
        this.vacuumMutex = new Object();
        this.addressLocker = new AddressLocker();
        this.pageMoveLock = new ReentrantReadWriteLock();
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
    public Header getHeader()
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
     * Get the read/write lock used to guard the user boundary. The read lock
     * of this read/write lock must be held by any operation attempting to
     * read a non-address page.
     * 
     * @return The read/write lock used to guard the user boundary.
     */
    public ReadWriteLock getPageMoveLock()
    {
        return pageMoveLock;
    }

    /**
     * Returns the address locker which will block a reallocation from
     * committing until the commit that freed an address has completed. This
     * prevents the reallocation from being overwritten by playback of the
     * journaled free.
     * 
     * @return The per pack address locker.
     */
    public AddressLocker getAddressLocker()
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
    public UserBoundary getUserBoundary()
    {
        return userBoundary;
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
