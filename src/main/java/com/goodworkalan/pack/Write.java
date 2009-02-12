package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;


final class Write
extends Operation
{
    private long address;
    
    private long position;
    
    public Write()
    {
    }
    
    public Write(long address, long position)
    {
        this.address = address;
        this.position = position;
    }
    
    private void commit(Sheaf sheaf, UserBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        AddressPage addresses = sheaf.getPage(address < 0 ? -address : address, AddressPage.class, new AddressPage());
        if (address > 0L && addresses.dereference(address) != 0)
        {
            long previous = 0L;
            for (;;)
            {
                UserPage user = userBoundary.dereference(sheaf, address);
                synchronized (user.getRawPage())
                {
                    if (user.free(address, dirtyPages) || previous == user.getRawPage().getPosition())
                    {
                        InterimPage interim = sheaf.getPage(position, InterimPage.class, new InterimPage());
                        synchronized (interim.getRawPage())
                        {
                            addresses.set(address, position, dirtyPages);
                            break;
                        }
                    }
                }
                // TODO After implementing move, tell me where this belongs.
                previous = user.getRawPage().getPosition();
            }
        }
        else
        {
            addresses.set(address < 0 ? -address : address, position, dirtyPages);
        }
    }

    @Override
    public void commit(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getBouquet().getUserBoundary(), player.getDirtyPages());
    }
    
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE + Pack.POSITION_SIZE;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.WRITE);
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