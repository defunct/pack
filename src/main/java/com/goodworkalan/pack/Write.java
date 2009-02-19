package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.Set;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

// TODO Comment.
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
    
    // TODO Comment.
    public Write(long address, long position)
    {
        this.address = address;
        this.position = position;
    }
    
    // TODO Comment.
    private void commit(Sheaf sheaf, UserBoundary userBoundary, Set<Long> freedBlockPages, Set<Long> allocatedBlockPages, DirtyPageSet dirtyPages)
    {
        AddressPage addresses = sheaf.getPage(address < 0 ? -address : address, AddressPage.class, new AddressPage());
        if (address > 0L && addresses.dereference(address) != 0)
        {
            long previous = 0L;
            for (;;)
            {
                BlockPage user = userBoundary.dereference(sheaf, address);
                synchronized (user.getRawPage())
                {
                    // TODO Free could also mean back reference points not back
                    // to address.  Remember, here you're not copying, just
                    // re-pointing.
                    boolean freed = user.getRawPage().getPosition() == position;
                    if (!freed)
                    {
                        freed = user.free(address, dirtyPages);
                        if (freed)
                        {
                            freedBlockPages.add(user.getRawPage().getPosition());
                        }
                    }
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
                // TODO After implementing move, tell me where this belongs.
                previous = user.getRawPage().getPosition();
            }
        }
        else
        {
            addresses.set(address < 0 ? -address : address, position, dirtyPages);
        }
        allocatedBlockPages.add(position);
    }

    // TODO Comment.
    @Override
    public void commit(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getBouquet().getUserBoundary(), player.getFreedBlockPages(), player.getAllocatedBlockPages(), player.getDirtyPages());
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
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE + Pack.POSITION_SIZE;
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
        bytes.putShort(Pack.WRITE);
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
