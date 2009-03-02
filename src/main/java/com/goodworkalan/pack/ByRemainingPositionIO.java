package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;

/**
 * An I/O strategy to write the page position of the first block of values in a
 * linked list of blocks containing pages with bytes available for allocation
 * for a specific aligned by remaining value.
 * <p>
 * This class is used to maintain the head page position of a liked list of
 * blocks of page positions for a specific aligned by remaining value in a by
 * remaining table.
 * 
 * @author Alan Gutierrez
 */
public class ByRemainingPositionIO implements LookupBlockPositionIO
{
    /** The page used by the by remaining table to track header values. */
    private final ByRemainingPage byRemainingPage;

    /**
     * The index of the header value for a specific aligned by remaining value.
     */
    private final int alignmentIndex;

    /**
     * Create a read/write strategy that writes the head value of a linked list
     * of lookup pages to the given by remaining page for the aligned by
     * remaining value at the given index.
     * 
     * @param byRemainingPage
     *            The by remaining page.
     * @param alignmentIndex
     *            The alginment index.
     */
    public ByRemainingPositionIO(ByRemainingPage byRemainingPage, int alignmentIndex)
    {
        this.byRemainingPage = byRemainingPage;
        this.alignmentIndex = alignmentIndex;
    }

    /**
     * Write the head value of the linked list of of lookup blocks.The dirty
     * page set is used to track any pages that are allocated or update by this
     * method.
     * 
     * @param position
     *            The page position of the first block in a linked list of
     *            lookup blocks.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void write(long position, DirtyPageSet dirtyPages)
    {
        byRemainingPage.setSlotPosition(alignmentIndex, position, dirtyPages);
    }

    /**
     * Read the head value of the linked list of of lookup blocks.
     * 
     * @return The page position of the first block in a linked list of lookup
     *         blocks.
     */
    public long read()
    {
        return byRemainingPage.getSlotPosition(alignmentIndex);
    }
}
