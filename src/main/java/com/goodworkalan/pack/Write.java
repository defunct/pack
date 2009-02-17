package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.Set;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

// FIXME Comment.
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

    @Override
    public void commit(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getBouquet().getUserBoundary(), player.getFreedBlockPages(), player.getAllocatedBlockPages(), player.getDirtyPages());
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
