package com.goodworkalan.pack;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.goodworkalan.sheaf.Sheaf;

/**
 * Gathers the various services so that they are in one place, but not all in
 * one class.
 * <p>
 * These services depend on each other, and many could be placed in a single
 * class, but the bouquet pattern allows us some modularity. Where one service
 * depends on another, that service is given as a parameter, rather than having
 * the services combined.
 * <p>
 * FIXME Comment.
 * 
 * @author Alan Gutierrez
 */
final class Bouquet
{
    /** The pack object. */
    private final Pack pack;
    
    /** The page manager of the pack to mutate. */
    private final Sheaf sheaf;
    
    /**
     * A synchronization strategy that prevents addresses that have been freed
     * from overwriting reallocations.
     */
    private final AddressLocker addressLocker;
    
    /**  Round block allocations to this alignment. */
    private final int alignment;

    private final TemporaryPool temporaryPool;
    
    private final AddressPagePool addressPagePool;
    
    private final InterimPagePool interimPagePool;
    
    private final UserPagePool userPagePool;
    
    /**
     * A mutex to ensure that only one thread at a time is vacuuming the pack.
     */
    private final Object vacuumMutex;

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

    /** The boundary between address pages and user data pages. */
    private final UserBoundary userBoundary;
    
    private final ReadWriteLock pageMoveLock;
    
    /** Housekeeping information stored at the head of the file. */
    private final Header header;

    /**
     * @param alignment
     *            The alignment to which all block allocations are rounded.
     * @param sheaf
     *            Page management.
     * @param header
     *            Housekeeping information stored at the head of the file.
     * @param addressPagePool
     * @param interimPagePool
     * @param temporaryFactory
     * @param userPagePool
     * @param staticPages
     *            A map of URIs that identify the addresses of static blocks
     *            specified at the creation of the pack.
     * @param userBoundary
     *            The boundary between address pages and user data pages.
     * @param interimBoundary
     *            The boundary between user data pages and interim data pages.
     */
    public Bouquet(Header header, Map<URI, Long> staticBlocks, UserBoundary userBoundary, Sheaf sheaf, AddressPagePool addressPagePool, TemporaryPool temporaryFactory)
    {
        this.pack = new Pack(this);
        this.alignment = header.getAlignment();
        this.header = header;
        this.staticBlocks = staticBlocks;
        this.journalHeaders = new PositionSet(Pack.FILE_HEADER_SIZE, header.getInternalJournalCount());
        this.userBoundary = userBoundary;
        this.sheaf = sheaf;
        this.addressPagePool = addressPagePool;
        this.userPagePool = new UserPagePool(new ByRemainingTable(this), header.getPageSize(), header.getAlignment());
        this.interimPagePool = new InterimPagePool();
        this.temporaryPool = temporaryFactory;
        this.vacuumMutex = new Object();
        this.addressLocker = new AddressLocker();
        this.pageMoveLock = new ReentrantReadWriteLock();
    }
    
    public Pack getPack()
    {
        return pack;
    }
    
    public Header getHeader()
    {
        return header;
    }
    
    /**
     * Return the alignment to which all block allocations are rounded.
     * 
     * @return The block alignment.
     */
    public int getAlignment()
    {
        return alignment;
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
    
    public Sheaf getSheaf()
    {
        return sheaf;
    }
    
    public AddressPagePool getAddressPagePool()
    {
        return addressPagePool;
    }

    public InterimPagePool getInterimPagePool()
    {
        return interimPagePool;
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

    public TemporaryPool getTemporaryPool()
    {
        return temporaryPool;
    }
    
    public UserPagePool getUserPagePool()
    {
        return userPagePool;
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

    public ReadWriteLock getPageMoveLock()
    {
        return pageMoveLock;
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
