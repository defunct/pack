package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Create an address page from the user region of the file.
 * <p>
 * The create address page operation contains the position of a page to turn
 * into an address page and the page position of the page where the current
 * contents of the user page were moved. If the 
 * 
 * @author Alan Gutierrez
 * 
 */
final class CreateAddressPage
extends Operation
{
    /** The position of the page to turn into an address page. */
    private long position;
    
    /** The position of the page where the user page was moved. */
    private long movedTo;
    
    /**
     * Construct an empty instance that can be populated with the
     * {@link #read(ByteBuffer) read} method.
     */
    public CreateAddressPage()
    {            
    }

    /**
     * Create an address page at the given page position writing out a moved
     * reference to the given position of the page where the user page was
     * moved.
     * 
     * @param position
     *            The position of the page to turn into an address page.
     * @param movedTo
     *            The position of the page where the user page was moved.
     */
    public CreateAddressPage(long position, long movedTo)
    {
        this.position = position;
        this.movedTo = movedTo;
    }

    /**
     * Create an address page at the position of this create address page
     * operation with a forward reference to the moved user page.
     * 
     * @param sheaf
     *            The page manager.
     * @param dirtyPages
     *            The dirty page set.
     */
    private void commit(Sheaf sheaf, DirtyPageSet dirtyPages)
    {
        AddressPage addresses = sheaf.setPage(position, AddressPage.class, new AddressPage(), dirtyPages);
        addresses.set(0, movedTo, dirtyPages);
    }

    /**
     * Create an address page at the position of this create address page
     * operation with a forward reference to the moved user page.
     * 
     * @param player
     *            The journal player.
     */
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