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
 */
class Move extends Operation
{
    /** The position of the source block page. */
    private long source;
    
    /** The position of the destination block page. */
    private long destination;
    
    /** The address of the last block in destination block page. */ 
    private long truncate;

    /**
     * Construct an empty instance that can be populated with the
     * {@link #read(ByteBuffer) read} method.
     */
    public Move()
    {
    }

    /**
     * Construct a move operation that copies the blocks from the source block
     * page to the destination block page after truncating the destination block
     * page so that it ends with the given truncate block address.
     * 
     * @param from
     *            The source page position.
     * @param to
     *            The destination page position.
     * @param truncate
     *            The current last block in the destination page.
     */
    public Move(long from, long to, long truncate)
    {
        this.source = from;
        this.destination = to;
        this.truncate = truncate;
    }

    /**
     * Copy the blocks from the source page to the destination page after
     * truncating the destination page so that the last block is the last block
     * indicated by the truncate address.
     * 
     * @param sheaf
     *            The underlying <code>Sheaf</code> page manager.
     * @param userBoundary
     *            The boundary between address pages and user pages.
     * @param dirtyPages
     *            The dirty page set.
     */
    private void commit(Sheaf sheaf, UserBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        BlockPage sourcePage = userBoundary.load(sheaf, source, BlockPage.class, new BlockPage());
        synchronized (sourcePage.getRawPage())
        {
            BlockPage destinationPage = userBoundary.load(sheaf, destination, BlockPage.class, new BlockPage());
            synchronized (destinationPage.getRawPage())
            {
                destinationPage.truncate(truncate, dirtyPages);
                for (long address : sourcePage.getAddresses())
                {
                    AddressPage addresses = userBoundary.load(sheaf, address, AddressPage.class, new AddressPage());
                    long current = addresses.dereference(address);
                    // Remember that someone else might have pointed the address
                    // at a newer version of the block, then committed, while
                    // this journal failed. During playback, we don't want to
                    // overwrite the new reference.
                    if (current == source || current == destination)
                    {
                        ByteBuffer read = sourcePage.read(address, null);
                        destinationPage.write(address, read, dirtyPages);
                        addresses.set(address, destination, dirtyPages);
                    }
                }
            }
        }
    }

    /**
     * Copy the blocks from the source page to the destination page after
     * truncating the destination page so that the last block is the last block
     * indicated by the truncate address.
     * 
     * @param player
     *            The journal player.
     */
    @Override
    public void execute(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getBouquet().getUserBoundary(), player.getDirtyPages());
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
        return Pack.SHORT_SIZE + Pack.LONG_SIZE * 3;
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
        bytes.putShort(MOVE);
        bytes.putLong(source);
        bytes.putLong(destination);
        bytes.putLong(truncate);
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
        this.source = bytes.getLong();
        this.destination = bytes.getLong();
        this.truncate = bytes.getLong();
    }
}
