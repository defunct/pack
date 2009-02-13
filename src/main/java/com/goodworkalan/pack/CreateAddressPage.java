package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class CreateAddressPage
extends Operation
{
    private long position;
    
    private long movedTo;
    
    public CreateAddressPage()
    {            
    }
    
    public CreateAddressPage(long position, long movedTo)
    {
        this.position = position;
        this.movedTo = movedTo;
    }
    
    @Override
    public void commit(Player player)
    {
        AddressPage addresses = player.getBouquet().getSheaf().setPage(position, AddressPage.class, new AddressPage(), player.getDirtyPages(), true);
        addresses.set(0, movedTo, player.getDirtyPages());
    }
    
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE * 2;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.CREATE_ADDRESS_PAGE);
        bytes.putLong(position);
        bytes.putLong(movedTo);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
        position = bytes.getLong(); 
        movedTo = bytes.getLong();
    }
}