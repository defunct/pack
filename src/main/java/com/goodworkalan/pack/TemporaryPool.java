package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.goodworkalan.lock.many.LatchSet;
import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Header;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Manages a pool of references to the addresses of temporary blocks. A
 * temporary block is a block that is used to store data in an intermediate step
 * during storage. A set of temporary blocks is made available to the client
 * programmer via the {@link Opener} when a file is reopened.
 * <p>
 * Why not use {@link LookupPagePool} to create a list of addresses in use?
 * Because the reserve a reference pattern is easy to journal. The lookup page
 * pool goes to great lengths to reduce its size, maintaining a linked list of
 * lookup blocks, sorting each of the blocks by the page position value, so that
 * the lookup page pool does a lot of writing, and the sorted blocks means
 * over-writing. These operations would require copying to journal correctly.
 * <p>
 * The temporary page pool reuses the {@link AddressPage} journal strategy of
 * reserving a page position, then setting it to the referenced value during
 * commit.
 * 
 * @author Alan Gutierrez
 */
class TemporaryPool
{
    /** The boundary between address pages and user pages. */
    private final AddressBoundary addressBoundary;
    
    /** A pool of references to address values. */
    private final ReferencePool referencePool;

    /**
     * A set of latches that prevents a reallocated temporary reference from
     * committing before the journal playback that freed the temporary reference
     * completes and commits.
     */
    private final LatchSet<Long> latchSet;

    /**
     * Map of temporary block addresses to temporary reference node addresses.
     */
    private final Map<Long, Long> temporaries;

    /**
     * Create a new temporary pool.
     * 
     * @param sheaf
     *            The page manager.
     * @param header
     *            The file header.
     * @param addressBoundary
     *            The boundary between address pages and user pages.
     * @param interimPagePool
     *            The interim page pool to use to allocate new reference pages.
     */
    public TemporaryPool(Sheaf sheaf, Header<Integer> header, AddressBoundary addressBoundary, InterimPagePool interimPagePool)
    {
        this.referencePool = new ReferencePool(sheaf, header, header.get(Housekeeping.FIRST_TEMPORARY_RESOURCE_PAGE), addressBoundary, interimPagePool);
        this.latchSet = new LatchSet<Long>(64);
        this.temporaries = new HashMap<Long, Long>();
        for (Map.Entry<Long, Long> mapping : referencePool.toMap(sheaf, addressBoundary).entrySet())
        {
            temporaries.put(mapping.getValue(), mapping.getKey());
        }
        this.addressBoundary = addressBoundary;
    }

    /**
     * Get a map of temporary block addresses to temporary reference node
     * addresses.
     * 
     * @return A map of temporary block addresses to temporary reference node
     *         addresses.
     */
    public synchronized Map<Long, Long> toMap()
    {
        return new HashMap<Long, Long>(temporaries);
    }

    /**
     * Allocate a new temporary reference. The temporary reference will be
     * reserved for use. During journal playback, the temporary reference will
     * be made permanent by writing a block address value to the temporary
     * reference with the {@link #commit(long, long, DirtyPageSet) commit}
     * method. The dirty page set is used to track any pages that are allocated
     * or update by this method.
     * 
     * @param dirtyPages
     *            The dirty page set.
     * @return The page position of a temporary reference.
     */
    public long allocate(DirtyPageSet dirtyPages)
    {
        return referencePool.allocate(dirtyPages);
    }

    /**
     * Commit a new temporary reference by writing a temporary block address at
     * the page position of the temporary reference. The dirty page set is used
     * to track any pages that are allocated or update by this method.
     * 
     * @param temporary
     *            The page position of the temporary reference.
     * @param address
     *            The temporary block address.
     * @param dirtyPages
     *            The dirty page set.
     */
    public synchronized void commit(long temporary, long address, DirtyPageSet dirtyPages)
    {
        latchSet.enter(temporary);
        AddressPage references = addressBoundary.load(temporary, AddressPage.class, new AddressPage());
        references.set(temporary, address, dirtyPages);
        temporaries.put(address, temporary);
    }

    /**
     * Free the given temporary reference of the given temporary block address.
     * If the block at the given address is indeed a temporary block, return the
     * page position of the temporary reference used to reference the temporary
     * block. Return zero if the block is not a temporary block. The dirty page
     * set is used to track any pages that are allocated or update by this
     * method.
     * <p>
     * This method will wait on a latch that protects a commit from overwriting
     * a freed temporary reference until the mutator that freed the temporary
     * reference completes the commit that freed the temporary reference.
     * 
     * @param address
     *            The temporary block address.
     * @param dirtyPages
     *            The dirty page set.
     * @return The page position of the temporary reference used to reference
     *         the temporary block or zero if the address does not refernece a
     *         temporary block.
     */
    public synchronized long free(long address, DirtyPageSet dirtyPages)
    {
        if (temporaries.containsKey(address))
        {
            long temporary = temporaries.get(address);
            latchSet.latch(temporary);
            AddressPage references = addressBoundary.load(temporary, AddressPage.class, new AddressPage());
            references.free(temporary, dirtyPages);
            temporaries.remove(address);
            return temporary;
        }
        return 0L;
    }

    /**
     * Rollback a temporary reference allocation by writing zero in the page
     * position of the temporary reference. The dirty page set is used to track
     * any pages that are allocated or update by this method.
     * <p>
     * This method will wait on a latch that protects a commit from overwriting
     * a freed temporary reference until the mutator that freed the temporary
     * reference completes the commit that freed the temporary reference.
     * 
     * @param temporary
     *            The temporary reference.
     * @param dirtyPages
     *            The dirty page set.
     */
    public synchronized void rollback(long temporary, DirtyPageSet dirtyPages)
    {
        if (temporaries.containsValue(temporary))
        {
            temporaries.values().remove(temporary);
        }
        latchSet.enter(temporary);
        AddressPage references = addressBoundary.load(temporary, AddressPage.class, new AddressPage());
        references.free(temporary, dirtyPages);
    }

    /**
     * Unlatch the freed temporary references in the given set of temporary
     * references so that new temporary block address values can be committed if
     * the temporary references were reallocated since they were freed.
     * 
     * @param temporaries
     *            A set of freed temporary references.
     */
    public void unlock(Set<Long> temporaries)
    {
        for (long position : temporaries)
        {
            latchSet.unlatch(position);
        }
    }
}
