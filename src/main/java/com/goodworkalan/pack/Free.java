package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.Set;

import com.goodworkalan.lock.many.LatchSet;
import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Free an address by assigning it a null page position and lock the address
 * from update until the journal playback completes. 
 * 
 * @author Alan Gutierrez
 */
final class Free
extends Operation
{
    /** The address to free. */
    private long address;
    
    /**
     * Construct an empty instance that can be populated with the
     * {@link #read(ByteBuffer) read} method.
     */
    public Free()
    {
    }

    /**
     * Create a free operation that will free the given address.
     * 
     * @param address
     *            The address to free.
     */
    public Free(long address)
    {
        this.address = address;
    }

    /**
     * Free the address of this free operation.
     * 
     * @param sheaf
     *            The page manager.
     * @param addressLocker
     *            Lock addresses against premature reassignment.
     * @param addressBoundary
     *            The boundary between address and non-address pages.
     * @param temporaryPool
     *            The pool of references to temporary blocks.
     * @param lockedAddresses
     *            The set of addresses locked against premature assignment by
     *            this journal playback.
     * @param lockedTemporaryAddresses
     *            The set of temporary address references locked against
     *            premature assignment by this journal playback.
     * @param freedBlockPages
     *            The set of block pages that have had one or more blocks freed
     *            during playback.
     * @param dirtyPages
     *            The dirty page set.
     */
    private void free(Sheaf sheaf, LatchSet<Long> addressLocker, AddressBoundary addressBoundary, TemporaryPool temporaryPool,
        Set<Long> lockedAddresses, Set<Long> lockedTemporaryAddresses, Set<Long> freedBlockPages, DirtyPageSet dirtyPages)
    {
        // Lock the address against reassignment until after this journal
        // playback completes.
        addressLocker.latch(address);
        lockedAddresses.add(address);

        // If there was a temporary reference freed, record tha the temporary
        // reference is locked against reassignment.
        long temporary;
        if ((temporary = temporaryPool.free(address, dirtyPages)) != 0L)
        {
            lockedTemporaryAddresses.add(temporary);
        }
        
        for (;;)
        {
            Dereference dereference = addressBoundary.dereference(sheaf, address);
            synchronized (dereference.getMonitor())
            {
                // Ensure that the page did not move since we dereferenced it.
                BlockPage user = dereference.getBlockPage(sheaf);
                if (user == null)
                {
                    continue;
                }

                // Moving synchronizes on the source page, then synchronizes on
                // the destination page. It synchronizes on the address page for
                // a block address. It frees a block from the source page, adds
                // a block to the destination page. Then it locks and updates
                // the address page. It leaves the address synchronization block
                // and continues with the next block address.

                // If we free the address, we know that we have not lost a race
                // against another page attempting to move the block.

                // The address might have been freed by a failed playback,
                // however.

                // If the previous address is different from the address of the
                // current page, we may have lost a race against another thread
                // that changed the address reference.

                // If the previous address is the same as the address of the
                // current page, we thought we lost a race against another
                // thread that changed the address reference, but this time
                // through we see that the address reference has not changed.

                user.free(address, dirtyPages);
                freedBlockPages.add(user.getRawPage().getPosition());
                
                break;
            }
        }
        
        AddressPage addresses = sheaf.getPage(address, AddressPage.class, new AddressPage());
        addresses.free(address, dirtyPages);
    }

    /**
     * Free the address of this free operation.
     * 
     * @param player
     *            The journal player.
     */
    @Override
    public void execute(Player player)
    {
        free(player.getBouquet().getSheaf(), player.getBouquet().getAddressLocker(), player.getBouquet().getAddressBoundary(), player.getBouquet().getTemporaryPool(), player.getLockedAddresses(), player.getLockedTemporaryReferences(), player.getFreedBlockPages(), player.getDirtyPages());
    }
    
    /**
     * Return the length of the operation in the journal including the type
     * flag.
     * 
     * @return The length of this operation in the journal.
     */
    @Override
    public int length()
    {
        return Pack.SHORT_SIZE + Pack.LONG_SIZE;
    }

    /**
     * Write the operation type flag and the operation data to the given byte
     * buffer.
     * 
     * @param bytes
     *            The byte buffer.
     */
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(FREE);
        bytes.putLong(address);
    }

    /**
     * Read the operation data but not the preceding operation type flag from
     * the byte buffer.
     * 
     * @param bytes
     *            The byte buffer.
     */
    @Override
    public void read(ByteBuffer bytes)
    {
        address = bytes.getLong();
    }
}
