package com.goodworkalan.pack;

import java.util.Map;
import java.util.Set;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

class TemporaryNodePool
{
    private final ReferencePool referencePool;
    
    private final AddressLocker temporaryLocker;

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
    public TemporaryNodePool(Sheaf sheaf, UserBoundary userBoundary, Header header)
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
        this.temporaryLocker = new AddressLocker();
        this.temporaries = referencePool.toMap(sheaf, userBoundary);
    }
    
    public long allocate(Sheaf sheaf, Header header, UserBoundary userBoundary, InterimPagePool interimPagePool, DirtyPageSet dirtyPages)
    {
        return referencePool.allocate(sheaf, header, userBoundary, interimPagePool, dirtyPages);
    }
    
    public synchronized void commit(long address, long temporary, Sheaf sheaf, UserBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        temporaryLocker.bide(temporary);
        AddressPage references = userBoundary.load(sheaf, temporary, AddressPage.class, new AddressPage());
        references.set(temporary, address, dirtyPages);
        temporaries.put(address, temporary);
    }

    public synchronized boolean free(long address, Sheaf sheaf, UserBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        if (temporaries.containsKey(address))
        {
            long temporary = temporaries.get(address);
            temporaryLocker.lock(temporary);
            AddressPage references = userBoundary.load(sheaf, temporary, AddressPage.class, new AddressPage());
            references.free(temporary, dirtyPages);
            temporaries.put(address, temporary);
            return true;
        }
        return false;
    }
    
    public void unlock(Set<Long> temporaries)
    {
        temporaryLocker.unlock(temporaries);
    }
}
