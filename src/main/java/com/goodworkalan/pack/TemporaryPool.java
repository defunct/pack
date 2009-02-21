package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.goodworkalan.lock.many.LatchSet;
import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Manages a pool of references to the addresses of temporary blocks. A
 * temporary block is a block that is used to store data in an intermediate step
 * during storage. A set of temporary blocks is made available to the client
 * programmer via the {@link Opener} when a file is reopened.
 * <p>
 * The temporary page pool manages a reference pool that...
 * <p>
 * FIXME Why is this not a linked list of sorted arrays of addresses?
 * 
 * @author Alan Gutierrez
 */
class TemporaryPool
{
    /** A pool of references to address values. */
    private final ReferencePool referencePool;

    /**
     * A set of latches that prevents a reallocated temporary reference from
     * committing before the journal playback that freed the temporary reference
     * completes and commits.
     */
    private final LatchSet<Long> temporaryLocker;

    /**
     * Map of temporary block addresses to temporary reference node addresses.
     */
    private final Map<Long, Long> temporaries;

    /**
     * Create a new temporary node factory.
     * 
     * @param temporaryNodes
     *            Map of temporary node addresses to byte buffers containing the
     *            address value at the temporary node position.
     */
    public TemporaryPool(Sheaf sheaf, UserBoundary userBoundary, Header header)
    {
        this.referencePool = new ReferencePool(sheaf, userBoundary, header)
        {
            @Override
            protected long getHeaderField(Header header)
            {
                return header.getFirstTemporaryNode();
            }
            
            @Override
            protected void setHeaderField(Header header, long position)
            {
                header.setFirstTemporaryNode(position);
            }
        };
        this.temporaryLocker = new LatchSet<Long>(64);
        this.temporaries = new HashMap<Long, Long>();
        for (Map.Entry<Long, Long> mapping : referencePool.toMap(sheaf, userBoundary).entrySet())
        {
            temporaries.put(mapping.getValue(), mapping.getKey());
        }
    }
    
    // TODO Comment.
    public synchronized Map<Long, Long> toMap()
    {
        return new HashMap<Long, Long>(temporaries);
    }
    
    // TODO Comment.
    public long allocate(Sheaf sheaf, Header header, UserBoundary userBoundary, InterimPagePool interimPagePool, DirtyPageSet dirtyPages)
    {
        return referencePool.allocate(sheaf, header, userBoundary, interimPagePool, dirtyPages);
    }
    
    // TODO Comment.
    public synchronized void commit(long address, long temporary, Sheaf sheaf, UserBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        temporaryLocker.enter(temporary);
        AddressPage references = userBoundary.load(sheaf, temporary, AddressPage.class, new AddressPage());
        references.set(temporary, address, dirtyPages);
        temporaries.put(address, temporary);
    }

    // TODO Comment.
    public synchronized long free(long address, Sheaf sheaf, UserBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        if (temporaries.containsKey(address))
        {
            long temporary = temporaries.get(address);
            temporaryLocker.latch(temporary);
            AddressPage references = userBoundary.load(sheaf, temporary, AddressPage.class, new AddressPage());
            references.free(temporary, dirtyPages);
            temporaries.remove(address);
            return temporary;
        }
        return 0L;
    }
    
    // TODO Comment.
    public void unlock(Set<Long> temporaries)
    {
        for (long position : temporaries)
        {
            temporaryLocker.unlatch(position);
        }
    }
}
