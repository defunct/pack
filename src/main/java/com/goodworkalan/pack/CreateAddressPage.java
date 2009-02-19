package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

//TODO Comment.
final class CreateAddressPage
extends Operation
{
    // TODO Comment.
    private long position;
    
    // TODO Comment.
    private long movedTo;
    
    // TODO Comment.
    public CreateAddressPage()
    {            
    }
    
    // TODO Comment.
    public CreateAddressPage(long position, long movedTo)
    {
        this.position = position;
        this.movedTo = movedTo;
    }
    
    // TODO Comment.
    private void commit(Sheaf sheaf, DirtyPageSet dirtyPages)
    {
        AddressPage addresses = sheaf.setPage(position, AddressPage.class, new AddressPage(), dirtyPages);
        addresses.set(0, movedTo, dirtyPages);
    }

    // TODO Comment.
    @Override
    public void commit(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getDirtyPages());
    }
    
    // TODO Comment.
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE * 2;
    }
    
    // TODO Comment.
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.CREATE_ADDRESS_PAGE);
        bytes.putLong(position);
        bytes.putLong(movedTo);
    }
    
    // TODO Comment.
    @Override
    public void read(ByteBuffer bytes)
    {
        position = bytes.getLong(); 
        movedTo = bytes.getLong();
    }
}