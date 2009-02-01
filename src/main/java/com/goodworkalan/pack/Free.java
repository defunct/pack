package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.Sheaf;


final class Free
extends Operation
{
    private long address;
    
    private long position;
    
    public Free()
    {
    }

    public Free(long address, long position)
    {
        this.address = address;
        this.position = position;
    }

    @Override
    public void commit(Player player)
    {
        Bouquet bouquet = player.getBouquet();
        // TODO Someone else can allocate the address and even the block
        // now that it is free and the replay ruins it. 
        Sheaf pager = bouquet.getSheaf();
        bouquet.getAddressLocker().lock(address);
        player.getAddressSet().add(address);
        // TODO Same problem with addresses as with temporary headers,
        // someone can reuse when we're scheduled to release.
        player.getBouquet().getTemporaryFactory().freeTemporary(player.getBouquet().getSheaf(), address, player.getDirtyPages());
        AddressPage addresses = pager.getPage(address, AddressPage.class, new AddressPage());
        addresses.free(address, player.getDirtyPages());
        UserPage user = pager.getPage(player.adjust(position), UserPage.class, new UserPage());
        user.waitOnMirrored();
        // TODO What is reserve and release about here?
        bouquet.getUserPagePool().getFreePageBySize().reserve(user.getRawPage().getPosition());
        user.free(address, player.getDirtyPages());
        bouquet.getUserPagePool().getFreePageBySize().release(user.getRawPage().getPosition(), user.getRawPage().getPosition());
        bouquet.getUserPagePool().returnUserPage(user);
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE + Pack.POSITION_SIZE;
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.FREE);
        bytes.putLong(address);
        bytes.putLong(position);
    }

    @Override
    public void read(ByteBuffer bytes)
    {
        address = bytes.getLong();
        position = bytes.getLong();
    }
}