package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.Sheaf;


final class Free
extends Operation
{
    private long address;
    
    public Free()
    {
    }

    public Free(long address)
    {
        this.address = address;
    }

    @Override
    public void commit(Player player)
    {
        Bouquet bouquet = player.getBouquet();
        Sheaf pager = bouquet.getSheaf();
        
        bouquet.getAddressLocker().lock(address);
        player.getAddresses().add(address);

        if (bouquet.getTemporaryFactory().freeTemporary(player.getBouquet().getSheaf(), address, bouquet.getTemporaryAddressLocker(), player.getDirtyPages()))
        {
            player.getTemporaryAddresses().add(address);
        }
        
        long previous = 0L;
        for (;;)
        {
            UserPage user = bouquet.getUserBoundary().dereference(bouquet.getSheaf(), address);
            if (user.free(address, player.getDirtyPages()) || user.getRawPage().getPosition() == previous)
            {
                // FIXME Moving will work this way, lock from page, lock to
                // page. Free from form page, add to to page. Then lock and
                // update address page, free lock. Then free user locks.

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

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE;
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.FREE);
        bytes.putLong(address);
    }

    @Override
    public void read(ByteBuffer bytes)
    {
        address = bytes.getLong();
    }
}
