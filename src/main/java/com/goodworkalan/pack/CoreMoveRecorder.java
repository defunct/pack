package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.pack.vacuum.MoveRecorder;
import com.goodworkalan.pack.vacuum.Vacuum;
import com.goodworkalan.sheaf.DirtyPageSet;

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
    
    /* (non-Javadoc)
     * @see com.goodworkalan.pack.MoveRecorder#getPageSize()
     */
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
        return bouquet.getAddressBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
    }

    /* (non-Javadoc)
     * @see com.goodworkalan.pack.MoveRecorder#getBytesRemaining(long)
     */
    public int getBytesRemaining(long position)
    {
        return getBlockPage(position).getRemaining();
    }

    /* (non-Javadoc)
     * @see com.goodworkalan.pack.MoveRecorder#getBlockSizes(long)
     */
    public Map<Long, Integer> getBlockSizes(long position)
    {
        Map<Long, Integer> map = new HashMap<Long, Integer>();
        BlockPage blocks = getBlockPage(position);
        synchronized (blocks.getRawPage())
        {
            for (long address : blocks.getAddresses())
            {
                map.put(address, blocks.getBlockSize(address));
            }
        }
        return map;
    }

    /* (non-Javadoc)
     * @see com.goodworkalan.pack.MoveRecorder#move(long, long)
     */
    public void move(long source, long destination)
    {
        moves.put(source, destination);
    }

    /* (non-Javadoc)
     * @see com.goodworkalan.pack.MoveRecorder#move(long)
     */
    public void move(long source)
    {
        BlockPage destnation = bouquet.getInterimPagePool().newInterimPage(bouquet.getSheaf(), BlockPage.class, new BlockPage(), dirtyPages, true);
        moves.put(source, destnation.getRawPage().getPosition());
    }
}
