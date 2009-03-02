package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.List;

import com.goodworkalan.sheaf.DirtyPageSet;

/**
 * An I/O strategy to read and write the header page position of the lookup page
 * used to allocate new lookup blocks for a by remaining table. The by remaining
 * table has a lookup page pool for each aligned by remaining size that it
 * tracks. This strategy manages the pages used to allocate the lookup blocks of
 * values. A lookup page for a specific block size can be used to allocate
 * blocks for any of the aligned by remaining values.
 * <p>
 * This strategy also calculates the list block sizes by starting with the
 * largest possible block that will fit on a page and dividing it in to until
 * the smallest size that can fit at least eight values. The first block page in
 * the lookup page pool will be the smallest size, the second page the second
 * smallest, and so on, until the full size is reached. Thereafter, all blocks
 * will be full pages.
 * 
 * @author Alan Gutierrez
 */
public class ByRemainingPagePositionIO implements LookupPagePositionIO
{
    /** The page used by the by remaining table to track header values. */
    private final ByRemainingPage byRemainingPage;
    
    /** A list of successive block sizes. */
    private final List<Integer> blockSizes;

    /**
     * Create an I/O strategy to read and write the header page position of the
     * lookup page used to allocate new lookup blocks for a by remaining table
     * using the given by remaining page to store the header values. The list of
     * block sizes is calculated according to the class documentation using the
     * given page size.
     * 
     * @param byRemainingPage
     *            The by remaining table housekeeping page.
     * @param pageSize
     *            The page size.
     */
    public ByRemainingPagePositionIO(ByRemainingPage byRemainingPage, int pageSize)
    {
        this.byRemainingPage = byRemainingPage;
        this.blockSizes = getSlotSizes(pageSize);
    }
    
    /**
     * Return the count of position blocks in a page for the given position
     * count.
     * 
     * @param pageSize
     *            The page size.
     * @param positionCount
     *            The number of positions in a position block in a page.
     * @return The number of position blocks that will fit on the page.
     */
    private static int getPositionBlockCount(int pageSize, int positionCount)
    {
        return ((Pack.INT_SIZE - pageSize) / Pack.LONG_SIZE) / positionCount;
    }
    
    /**
     * Create a list of slots sizes, starting from the full page size less the
     * slot header, then a sequence that divides the previous value in half,
     * ending with a slot size that is no less than 8 slots.
     * 
     * @param pageSize
     *            The page size.
     * @param alignment
     *            The block alignment.
     * @return A list of
     */
    private static List<Integer> getSlotSizes(int pageSize)
    {
        List<Integer> blockSizes = new ArrayList<Integer>();
        int blockCount = 1;
        for (;;)
        {
            int slotSize = getPositionBlockCount(pageSize, blockCount);
            if (slotSize < 8)
            {
                break;
            }
            blockSizes.add(slotSize);
            blockCount *= 2;
        }
        return blockSizes;
    }

    /**
     * Get the list of block sizes that specifies the size of each consecutive
     * block allocation.
     * 
     * @return The list of block sizes.
     */
    public List<Integer> getBlockSizes()
    {
        return blockSizes;
    }

    /**
     * Write the given page position value of the lookup page that will be used
     * to allocate blocks of the block size from the list of block sizes at the
     * given index. The dirty page set is used to track any pages that are
     * allocated or update by this method.
     * 
     * @param blockSizeIndex
     *            The index of the block size in the block size list.
     * @param position
     *            The page position of the lookup page.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void write(int blockSizeIndex, long position, DirtyPageSet dirtyPages)
    {
        byRemainingPage.setAllocSlotPosition(blockSizeIndex, position, dirtyPages);
    }

    /**
     * Read the page position value of the lookup page that will be used to
     * allocate blocks of the block size from the list of block sizes at the
     * given index.
     * 
     * @param blockSizeIndex
     *            The index of the block size in the block size list.
     * @return The page position of the lookup pages used to allocate blocks of
     *         the block size from the list of block sizes at the given index.
     */
    public long read(int blockSizeIndex)
    {
        return byRemainingPage.getAllocSlotPosition(blockSizeIndex);
    }
}