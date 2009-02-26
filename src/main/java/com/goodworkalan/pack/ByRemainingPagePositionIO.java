package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.List;

import com.goodworkalan.sheaf.DirtyPageSet;

// TODO Document.
public class ByRemainingPagePositionIO implements LookupPagePositionIO
{
    private final ByRemainingPage byRemainingPage;
    
    // TODO Document.
    private final List<Integer> blockSizes;

    // TODO Document.
    public ByRemainingPagePositionIO(ByRemainingPage byRemainingPage, int pageSize, int alignment)
    {
        this.byRemainingPage = byRemainingPage;
        this.blockSizes = getSlotSizes(pageSize, alignment);
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
    private static List<Integer> getSlotSizes(int pageSize, int alignment)
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
    
    // TODO Document.
    public List<Integer> getBlockSizes()
    {
        return blockSizes;
    }
    
    // TODO Document.
    public void write(int blockSizeIndex, long position, DirtyPageSet dirtyPages)
    {
        byRemainingPage.setAllocSlotPosition(blockSizeIndex, position, dirtyPages);
    }
    
    // TODO Document.
    public long read(int blockSizeIndex)
    {
        return byRemainingPage.getAllocSlotPosition(blockSizeIndex);
    }
}