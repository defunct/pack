package com.goodworkalan.pack.vacuum;

import java.util.Map;

/**
 * Used by {@link Vacuum} to record the moves necessary to optimize the size a
 * file. The interface provides size information for pages and blocks as well as
 * methods to move blocks from one page to another.
 * 
 * @author Alan Gutierrez
 */
public interface MoveRecorder
{
    /**
     * Get the size of a page in the file.
     * 
     * @return The page size.
     */
    public int getPageSize();

    /**
     * Get the bytes remaining in the block page at the given position.
     * 
     * @param position
     *            The block page position.
     * @return The bytes remaining in the block page.
     */
    public int getBytesRemaining(long position);

    /**
     * Return a map of addresses to block sizes in the block page at the given
     * position.
     * 
     * @param position
     *            The position of the block page.
     * @return A map of addresses to block sizes.
     */
    public Map<Long, Integer> getBlockSizes(long position);

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
    public void move(long source, long destination);

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
    public void move(long source);
}