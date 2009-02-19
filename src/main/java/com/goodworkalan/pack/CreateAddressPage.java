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
    
    /**
     * Construct an empty instance that can be populated with the
     * {@link #read(ByteBuffer) read} method.
     */
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
    public void execute(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getDirtyPages());
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
        return Pack.SHORT_SIZE + Pack.LONG_SIZE * 2;
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
        bytes.putShort(CREATE_ADDRESS_PAGE);
        bytes.putLong(position);
        bytes.putLong(movedTo);
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
        position = bytes.getLong(); 
        movedTo = bytes.getLong();
    }
}