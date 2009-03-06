package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.pack.vacuum.MoveRecorder;
import com.goodworkalan.pack.vacuum.Vacuum;
import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.RawPage;

/**
 * Used by {@link Vacuum} to record the moves necessary to vacuum a
 * <code>Pack</code>.
 * <p>
 * This class exposes an interface to determine bytes remaining of 
 * 
 * @author Alan Gutierrez
 */
class CoreMoveRecorder implements MoveRecorder
{
    /** A bouquet of services. */
    private final Bouquet bouquet;
    
    /** The map of block moves. */
    private final Map<Long, Long> moves;

    /** The set of dirty pages. */
    private DirtyPageSet dirtyPages;

    /**
     * Create a new move recorder using the given bouquet of services and
     * recording block moves in the given map.
     * 
     * @param bouquet
     *            The bouquet of services.
     * @param moves
     *            A map of block moves.
     */
    CoreMoveRecorder(Bouquet bouquet, Map<Long, Long> moves)
    {
        this.bouquet = bouquet;
        this.moves = moves;
    }
    
    // TODO Document.
    public int getPageSize()
    {
        return bouquet.getSheaf().getPageSize();
    }

    /**
     * Get the block page at the given position.
     * 
     * @param position
     *            The position of the block page.
     * @return The block page.
     */
    private BlockPage getBlockPage(long position)
    {
        return bouquet.getAddressBoundary().load(position, BlockPage.class, new BlockPage());
    }

    // TODO Document.
    public int getBytesRemaining(long position)
    {
        return getBlockPage(position).getRemaining();
    }

    // TODO Document.
    public Map<Long, Integer> getBlockSizes(long position)
    {
        Map<Long, Integer> map = new HashMap<Long, Integer>();
        BlockPage blocks = getBlockPage(position);
        RawPage rawPage = blocks.getRawPage_();
        rawPage.getLock().lock();
        try
        {
            for (long address : blocks.getAddresses())
            {
                map.put(address, blocks.getBlockSize(address));
            }
        }
        finally
        {
            rawPage.getLock().unlock();
        }
        return map;
    }

    // TODO Document.
    public void move(long source, long destination)
    {
        moves.put(source, destination);
    }

    // TODO Document.
    public void move(long source)
    {
        BlockPage destnation = bouquet.getInterimPagePool().newInterimPage(BlockPage.class, new BlockPage(), dirtyPages, true);
        moves.put(source, destnation.getRawPage_().getPosition());
    }
}
