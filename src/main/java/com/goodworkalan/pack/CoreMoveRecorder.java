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
    
    /**
     * Get the size of a page in the file.
     * 
     * @return The page size.
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
        return bouquet.getAddressBoundary().load(position, BlockPage.class, new BlockPage());
    }

    /**
     * Get the bytes remaining in the block page at the given position.
     * 
     * @param position
     *            The block page position.
     * @return The bytes remaining in the block page.
     */
    public int getBytesRemaining(long position)
    {
        return getBlockPage(position).getRemaining();
    }

    /**
     * Get a map of addresses to block sizes for the page at the given position.
     * 
     * @param position
     *            The page position.
     * @return A map of addresses to block sizes.
     */
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

    /**
     * Move the all the blocks from the given <code>source</code> page to the
     * given <code>destination</code> page.
     * <p>
     * All of the blocks in the source block page will be appended to the
     * destination block page. At the end of the vacuum, the source block page
     * will be returned to the set of free interim pages.
     * 
     * @param source
     *            The page position of the source page.
     * @param destination
     *            The page position of the destination page.
     */
    public void move(long source, long destination)
    {
        moves.put(source, destination);
    }

    /**
     * Move all the blocks from the given <code>source</code> page to a newly
     * allocated block page.
     * <p>
     * This method is used to close the gaps left by freed blocks. When a block
     * is freed, if it is not the last block in the list of blocks on a page, it
     * leaves a gap, so that the freed bytes cannot be used, since blocks are
     * only ever appended to block pages. This method will reclaim the space by
     * appending all the blocks in the source page to a new empty page.
     * <p>
     * Gaps can also be reclaimed by copying blocks from a page that has gaps,
     * to a page that that has no gaps using the {@link #move(long, long)}
     * method.
     * 
     * @param source
     *            The page position of the source page.
     */
    public void move(long source)
    {
        BlockPage destnation = bouquet.getInterimPagePool().newInterimPage(BlockPage.class, new BlockPage(), dirtyPages, true);
        moves.put(source, destnation.getRawPage_().getPosition());
    }
}
