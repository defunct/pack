package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.Sheaf;

// TODO Comment.
final class Free
extends Operation
{
    // TODO Comment.
    private long address;
    
    /**
     * Construct an empty instance that can be populated with the
     * {@link #read(ByteBuffer) read} method.
     */
    public Free()
    {
    }

    // TODO Comment.
    public Free(long address)
    {
        this.address = address;
    }

    // TODO Comment.
    @Override
    public void commit(Player player)
    {
        Bouquet bouquet = player.getBouquet();
        Sheaf pager = bouquet.getSheaf();
        
        bouquet.getAddressLocker().lock(address);
        player.getAddresses().add(address);

        long temporary;
        if ((temporary = bouquet.getTemporaryPool().free(address, player.getBouquet().getSheaf(), bouquet.getUserBoundary(), player.getDirtyPages())) != 0L)
        {
            player.getTemporaryAddresses().add(temporary);
        }
        
        long previous = 0L;
        for (;;)
        {
            BlockPage user = bouquet.getUserBoundary().dereference(bouquet.getSheaf(), address);
            if (user.free(address, player.getDirtyPages()) || user.getRawPage().getPosition() == previous)
            {
                player.getFreedBlockPages().add(user.getRawPage().getPosition());

                // Moving will work this way, lock from page, lock to page. Free
                // from form page, add to to page. Then lock and update address
                // page, free lock. Then free user locks.

                // So, when we are here, the page is free because it moved, then
                // we try again. If we try again, it is free, and it hasn't
                // moved, then we've freed it already in a previous attempt to
                // playback the journal.
            
                break;
            }
            previous = user.getRawPage().getPosition();
        }
        
        AddressPage addresses = pager.getPage(address, AddressPage.class, new AddressPage());
        addresses.free(address, player.getDirtyPages());
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
