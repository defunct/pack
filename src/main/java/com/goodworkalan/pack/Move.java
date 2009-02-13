package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Move the blocks in a user block page to a new user block page.
 * <p>
 * The blocks are not freed from the source page because the journal playback
 * may fail and the journal may be replayed. When the journal is replayed we
 * need all of the source blocks. We ensure a good copy from source to
 * destination by truncating the destination after its pre-commit last address
 * and appending over the copies from the failed playback.
 * <p>
 * The blocks will be freed when the source page is marked as empty and given to
 * the interim page pool.
 * 
 * @author Alan Gutierrez
 * 
 */
class Move extends Operation
{
    private long from;
    
    private long to;
    
    private long truncateAt;

    public Move()
    {
    }
    
    public Move(long from, long to, long truncateAt)
    {
        this.from = from;
        this.to = to;
        this.truncateAt = truncateAt;
    }
    
    private void commit(Sheaf sheaf, UserBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        BlockPage source = userBoundary.load(sheaf, from, BlockPage.class, new BlockPage());
        synchronized (source.getRawPage())
        {
            BlockPage destination = userBoundary.load(sheaf, to, BlockPage.class, new BlockPage());
            synchronized (destination.getRawPage())
            {
                destination.truncate(truncateAt, dirtyPages);
                for (long address : source.getAddresses())
                {
                    AddressPage addresses = userBoundary.load(sheaf, address, AddressPage.class, new AddressPage());
                    long current = addresses.dereference(address);
                    // Remember that someone else might have pointed the address
                    // at a newer version of the block, then committed, while
                    // this journal failed. During playback, we don't want to
                    // overwrite the new reference.
                    if (current == from || current == to)
                    {
                        ByteBuffer read = source.read(address, null);
                        destination.write(address, read, dirtyPages);
                        addresses.set(address, to, dirtyPages);
                    }
                }
            }
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
        return Pack.FLAG_SIZE + Pack.POSITION_SIZE * 3;
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.MOVE);
        bytes.putLong(from);
        bytes.putLong(to);
        bytes.putLong(truncateAt);
    }

    @Override
    public void read(ByteBuffer bytes)
    {
        this.from = bytes.getLong();
        this.to = bytes.getLong();
        this.truncateAt = bytes.getLong();
    }
}
