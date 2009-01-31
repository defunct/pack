package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

public class TemporaryServer
{
    /**
     * Map of temporary node addresses to byte buffers containing the address
     * value at the temporary node position.
     */
    private final Map<Long, ByteBuffer> temporaryNodes;
    /**
     * Map of temporary block addresses to temporary reference node addreses.
     */
    private final Map<Long, Long> temporaries;

    /**
     * Create a new temporary node factory.
     * 
     * @param temporaryNodes
     *            Map of temporary node addresses to byte buffers containing the
     *            address value at the temporary node position.
     */
    public TemporaryServer(Map<Long, ByteBuffer> temporaryNodes)
    {
        this.temporaryNodes = temporaryNodes;
        this.temporaries = temporaries(temporaryNodes);
    }

    /**
     * Create a journal entry that will write a temporary node reference for the
     * given block address. The temporary journal entry is also used to rollback
     * the assignment of a temporary reference node to the temporary block, if
     * should the mutator rollback.
     * <p>
     * This method will assign the given address to a free temporary node
     * reference, If there is no free temporary node reference, it will allocate
     * one by creating a mutator for the sole purpose of extending the list.
     * <p>
     * The list of temporary reference nodes will grow but never shrink. The
     * temporary reference nodes will be reused when temporary blocks are freed.
     * 
     * @param address
     *            The address of the temporary block.
     * 
     * @see Temporary
     */
    public Temporary getTemporary(MutatorFactory mutatorFactory, long address)
    {
        // Synchronize temporary list manipulation on the temporary node map. 
        Temporary temporary = null;
        synchronized (temporaryNodes)
        {
            BUFFERS: for (;;)
            {
                // Find an empty temporary reference node, or failing that,
                // take node of the last temporary reference node in the linked
                // list of temporary reference nodes in order to append a new
                // temporary reference node later.
                Map.Entry<Long, ByteBuffer> last = null;
                for (Map.Entry<Long, ByteBuffer> entry : temporaryNodes.entrySet())
                {
                    ByteBuffer bytes = entry.getValue();
                    if (bytes.getLong(Pack.ADDRESS_SIZE) == 0L)
                    {
                        last = entry;
                    }
                    else if (bytes.getLong(0) == 0L)
                    {
                        temporaries.put(address, entry.getKey());
                        bytes.putLong(0, Long.MAX_VALUE);
                        temporary = new Temporary(address, entry.getKey());
                        break BUFFERS;
                    }
                }
                
                Mutator mutator = mutatorFactory.mutate();

                long next = mutator.allocate(Pack.ADDRESS_SIZE * 2);
                
                ByteBuffer bytes = mutator.read(next);
                while (bytes.remaining() != 0)
                {
                    bytes.putLong(0L);
                }
                bytes.flip();
                
                mutator.write(next, bytes);
    
                last.getValue().clear();
                last.getValue().putLong(Pack.ADDRESS_SIZE, next);
                
                mutator.write(last.getKey(), last.getValue());
                
                mutator.commit();
                
                temporaryNodes.put(next, bytes);
            }
        }
    
        return temporary;
    }

    /**
     * Set the temporary node at the given temporary node position to reference
     * the given block address. This method is used by the temporary journal
     * entry to set the value of the temporary reference node.
     * 
     * @param address
     *            The temporary block address.
     * @param temporary
     *            The address of the temporary reference node.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    public void setTemporary(Sheaf pager, long address, long temporary, DirtyPageSet dirtyPages)
    {
        // Synchronize temporary list manipulation on the temporary node map. 
        synchronized (temporaryNodes)
        {
            ByteBuffer bytes = temporaryNodes.get(temporary);
            bytes.putLong(0, address);
            bytes.clear();
    
            // Use the checked address dereference to find the  
            AddressPage addresses = pager.getPage(temporary, AddressPage.class, new AddressPage());
            long lastPosition = 0L;
            for (;;)
            {
                long position = addresses.dereference(temporary);
                if (lastPosition == position)
                {
                    throw new IllegalStateException();
                }
                UserPage user = pager.getPage(position, UserPage.class, new UserPage());
                synchronized (user.getRawPage())
                {
                    if (user.write(temporary, bytes, dirtyPages))
                    {
                        break;
                    }
                }
                lastPosition = position;
            }
        }
    }

    /**
     * Free a temporary block reference, setting the block reference to zero,
     * making it available for use to reference another temporary block.
     * 
     * @param address
     *            The address of the temporary block.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void freeTemporary(Sheaf pager, long address, DirtyPageSet dirtyPages)
    {
        // Synchronize temporary list manipulation on the temporary node map. 
        synchronized (temporaryNodes)
        {
            Long temporary = temporaries.get(address);
            if (temporary != null)
            {
                setTemporary(pager, 0L, temporary, dirtyPages);
            }
        }
    }

    /**
     * Return a temporary block reference to the pool of temporary block
     * references as the result of a rollback of a commit.
     * 
     * @param address
     *            The address of the temporary block.
     * @param temporary
     *            The address of temporary reference node.
     */
    public void rollbackTemporary(long address, long temporary)
    {
        // Synchronize temporary list manipulation on the temporary node map. 
        synchronized (temporaryNodes)
        {
            temporaries.remove(address);
            temporaryNodes.get(temporary).putLong(0, 0L);
        }
    }

    /**
     * Create a map of temporary block addresses to the address of their
     * temporary reference node.
     * 
     * @param temporaryNodes
     *            Map of temporary reference node addresses to byte buffers
     *            containing the address value at the temporary node position.
     * @return A map of temporary block addresses to temporary reference node
     *         addresses.
     */
    private static Map<Long, Long> temporaries(Map<Long, ByteBuffer> temporaryNodes)
    {
        Map<Long, Long> temporaries = new HashMap<Long, Long>();
        for (Map.Entry<Long, ByteBuffer> entry : temporaryNodes.entrySet())
        {
            ByteBuffer bytes = entry.getValue();
            long address = bytes.getLong();
            if (address != 0L)
            {
                temporaries.put(address, entry.getKey());
            }
        }
        return temporaries;
    }

}
