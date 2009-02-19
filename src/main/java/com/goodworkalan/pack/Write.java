package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.Set;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Update an address to reference a block allocation or isolated block write.
 * <p>
 * A write operation contains an address and the page position of a block page
 * containing the block to assign to that address.
 * <p>
 * In the case of an allocation, the write will update the address to reference
 * the newly allocated block.
 * <p>
 * In the case of a write, the write operation will dereference the current
 * address, free the block it its current page, then it will update the address
 * to reference the newly allocated block.
 * 
 * @author Alan Gutierrez
 */
final class Write
extends Operation
{
    /** The address of the write. */
    private long address;
    
    /** The position of the interim block of the write. */
    private long position;
    
    /**
     * Construct an empty instance that can be populated with the
     * {@link #read(ByteBuffer) read} method.
     */
    public Write()
    {
    }

    /**
     * Create a write address that will update the given address to reference
     * the block for that address in the block page at the given position.
     * 
     * @param address
     *            The address.
     * @param position
     *            The position of the block page.
     */
    public Write(long address, long position)
    {
        this.address = address;
        this.position = position;
    }

    /**
     * Update the address of this write operation to reference the block in the
     * the block page at the position this write operation.
     * 
     * @param sheaf
     *            The page manager.
     * @param userBoundary
     *            The boundary between the address pages and user pages.
     * @param freedBlockPages
     *            The set of blocks that have had pages freed during this
     *            playback of the journal.
     * @param allocatedBlockPages
     *            The pages with allocated blocks and isolated write blocks for
     *            this journal.
     * @param dirtyPages
     *            The dirty page set.
     */
    private void commit(Sheaf sheaf, UserBoundary userBoundary, Set<Long> freedBlockPages, Set<Long> allocatedBlockPages, DirtyPageSet dirtyPages)
    {
        // Get the address page for the address.
        AddressPage addresses = sheaf.getPage(address < 0 ? -address : address, AddressPage.class, new AddressPage());

        // If the address is greater than zero and the dereferenced addres is
        // does not reference a null page position.
        if (address > 0L && addresses.dereference(address) != 0L)
        {
            long previous = 0L;
            for (;;)
            {
                // Get the adjusted user page block page.
                BlockPage user = userBoundary.dereference(sheaf, address);
                // FIXME What if page moves here? What if it changes type? 
                synchronized (user.getRawPage())
                {
                    if (user.getRawPage().getPage() == user)
                    {
                        // We may have already updated the address during a
                        // failed journal playback.
                        boolean freed = user.getRawPage().getPosition() == position;
                        if (!freed)
                        {
                            // Free the existing block. We may have already
                            // freed the block but not updated the address
                            // during a failed playback, or we may have lost a
                            // race against another thread that is chaing the
                            // address reference.
                            freed = user.free(address, dirtyPages);
                            if (freed)
                            {
                                // Record the block page as containing freed
                                // blocks.
                                freedBlockPages.add(user.getRawPage().getPosition());
                            }
                        }

                        // If we have freed the address, then we know that the
                        // current page position referenced by the address is
                        // indeed correct.

                        // The address might have been freed by a failed
                        // playback, however.

                        // If the previous address is different from the address
                        // of the current page, we may have lost a race against
                        // another thread that changed the address refernece.

                        // If the previous address is the same as the address of
                        // the current page, we thought we lost a race against
                        // another thread that changed the address reference,
                        // but this time through we see that the address
                        // reference has not changed.

                        if (freed || previous == user.getRawPage().getPosition())
                        {
                            BlockPage interim = sheaf.getPage(position, BlockPage.class, new BlockPage());
                            synchronized (interim.getRawPage())
                            {
                                addresses.set(address, position, dirtyPages);
                                break;
                            }
                        }
                    }
                    previous = user.getRawPage().getPosition();
                }
            }
        }
        else
        {
            addresses.set(address < 0 ? -address : address, position, dirtyPages);
        }
        allocatedBlockPages.add(position);
    }

    /**
     * Update the address of this write operation to reference the block in the
     * the block page at the position this write operation.
     * 
     * @param player
     *            The journal player.
     */
    @Override
    public void execute(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getBouquet().getUserBoundary(), player.getFreedBlockPages(),
            player.getAllocatedBlockPages(), player.getDirtyPages());
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
        return Pack.SHORT_SIZE + Pack.LONG_SIZE + Pack.LONG_SIZE;
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
        bytes.putShort(WRITE);
        bytes.putLong(address);
        bytes.putLong(position);
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
        position = bytes.getLong();
    }
}
