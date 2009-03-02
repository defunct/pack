package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.List;

import com.goodworkalan.pack.vacuum.ByRemaining;
import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;
import com.goodworkalan.sheaf.Sheaf;

/**
 * A table to lookup pages by the amount of bytes remaining for block
 * allocation. The table rounds the amount remaining in a given page down to the
 * nearest block alignment, then stores it in a set for that value. Use the
 * {@link #bestFit(int)} method to find a page that will fit a given block size.
 * Use {@link #reserve(long)} to prevent a page from being returned...
 * <p>
 * A table of pages ordered by size that performs a best fit lookup, returning
 * the page in the collection with the least amount of free space that will
 * accommodate a block of a given size.
 * <p>
 * <ol>
 * <li>A position is allocated by a mutator, it tracks that position using the
 * addresses map in the mutator. The position does not change form the point of
 * allocation.</li>
 * <li>At commit, the position is written into the address page as the position
 * value at the time of allocation.</li>
 * <li>Vacuum may add the page to the by remaining table, if it does, it
 * <strong>must</strong> be added using the page position that the mutator wrote
 * into the address pages.</li>
 * <li>If the user frees a block, the address will dereference the page position
 * at allocation. The unadjusted page position <strong>must</strong> be reported
 * in the free page set.</li>
 * <li>Freed pages will be removed from the by remaining table, they will be
 * purged then either added or vacuumed. Once they are removed from the by
 * remaining table, the adjusted values can be added.</li>
 * </ol>
 * 
 * @author Alan Gutierrez
 */
final class ByRemainingTable implements ByRemaining
{
    /** The bouquet of services. */
    private final Sheaf sheaf;

    private final AddressBoundary userBoundary;

    private final InterimPagePool interimPagePool;

    private final int alignment;

    private final int maximumBlockSize;

    /**
     * A page for storing by remaining pages grouped by alignment and slot pages
     * grouped by slot size.
     */
    private ByRemainingPage byRemainingPage;

    /**
     * A list of lookup page pools, one for each aligned by remaining value.
     */
    private final List<LookupPagePool> lookupPagePools;

    /**
     * The dirty page set to use when writing the by remaining page and slot
     * pages.
     */
    private final DirtyPageSet dirtyPages;

    /**
     * Create a table to lookup pages by the amount of bytes remaining for block
     * allocation. The table will create sets of pages in a lookup table. The
     * index of to use to lookup a page is determined by rounding a block size
     * down to the nearest alignment, then dividing that number by the
     * alignment. The lookup table will have <code>(pageSize / alignment)</code>
     * slots, each containing a page set.
     * 
     * @param pageSize
     *            The page size.
     * @param alignment
     *            The block alignment.
     */
    public ByRemainingTable(Sheaf sheaf, AddressBoundary userBoundary,
            InterimPagePool interimPagePool, int alignment,
            int maximumBlockSize, DirtyPageSet dirtyPages)
    {
        this.sheaf = sheaf;
        this.userBoundary = userBoundary;
        this.interimPagePool = interimPagePool;
        this.alignment = alignment;
        this.dirtyPages = new DirtyPageSet();
        this.lookupPagePools = new ArrayList<LookupPagePool>();
        this.maximumBlockSize = maximumBlockSize;
    }

    /**
     * Populate the list of position pages indexed by alignment index.
     */
    private void createSlotPagePools()
    {
        int pageSize = sheaf.getPageSize();
        int alignmentCount = pageSize / alignment;
        LookupPagePositionIO lookupPagePositionIO = new ByRemainingPagePositionIO(
                byRemainingPage, pageSize);
        for (int i = 0; i < alignmentCount; i++)
        {
            lookupPagePools.add(new LookupPagePool(sheaf, userBoundary,
                    interimPagePool, lookupPagePositionIO,
                    new ByRemainingPositionIO(byRemainingPage, i)));
        }
    }

    /**
     * Load the by remaining table from the given position or create a new by
     * remaining table if the position is zero.
     * 
     * @param position
     *            The position of the by remaining table header page.
     */
    public void load(long position)
    {
        if (byRemainingPage == null)
        {
            if (position == 0L)
            {
                byRemainingPage = interimPagePool.newInterimPage(sheaf,
                        ByRemainingPage.class, new ByRemainingPage(),
                        dirtyPages, false);
            }
            else
            {
            }
        }
        createSlotPagePools();
    }

    /**
     * Add the page position and amount of bytes remaining for block allocation
     * of the given block page to the table. If the amount of bytes remaining
     * for block allocation rounds down to zero, the page position is not added
     * to the table.
     * <p>
     * If the page position of the block has been reserved, the page will be put
     * into a waiting state until the page has been released, at which point it
     * will be added.
     * 
     * @param blocks
     *            The block page to add.
     */
    public void add(BlockPage blocks)
    {
        add(blocks.getRawPage().getPosition(), blocks.getRemaining());
    }

    /**
     * Round the remaining bytes down to the nearest alignment.
     * 
     * @param remaining
     *            A count of bytes remaining.
     * @return The remaining bytes down to the nearest alignment.
     */
    private int alignRemaining(int remaining)
    {
        return remaining / alignment * alignment;
    }

    /**
     * Add the page at the given position with the given amount of bytes
     * remaining for block allocation to the table. If the amount of bytes
     * remaining for block allocation rounds down to zero, the page position is
     * not added to the table.
     * <p>
     * If the page position has been reserved, the page will be put into a
     * waiting state until the page has been released, at which point it will be
     * added.
     * 
     * @param position
     *            The page position.
     * @param remaining
     *            The count of bytes remaining for block allocation.
     */
    public void add(long position, int remaining)
    {
        load(0L);

        int aligned = remaining / alignment * alignment;
        if (aligned != 0)
        {
            int alignmentIndex = aligned / alignment;
            lookupPagePools.get(alignmentIndex).add(remaining, dirtyPages);
        }
    }

    /**
     * Remove the given page position from the table.
     * <p>
     * Returns the amount of blocks remaining rounded down to the nearest
     * alignment. This is used by the {@link ByRemainingTableMoveTracker} to
     * relocate the page and amount remaining in the table, when the page moves.
     * 
     * @param position
     *            The page position.
     */
    public void remove(long position, int remaining)
    {
        int aligned = remaining / alignment * alignment;
        int alignmentIndex = alignment / aligned;
        lookupPagePools.get(alignmentIndex).remove(position, dirtyPages);
    }

    /**
     * @see com.goodworkalan.pack.ByRemaining#bestFit(int)
     */
    public long bestFit(int blockSize)
    {
        int aligned = (blockSize + alignment - 1) / alignment * alignment;

        // Return zero there is no chance of a fit.
        if (aligned > maximumBlockSize - alignment)
        {
            return 0L;
        }

        long position = 0L;

        int pageSize = sheaf.getPageSize();
        for (int alignmentIndex = aligned / alignment; position == 0L
                && alignmentIndex < pageSize / alignment; alignmentIndex++)
        {
            for (;;)
            {
                position = lookupPagePools.get(alignmentIndex).poll(dirtyPages);
                if (position == 0L)
                {
                    break;
                }
                long adjusted = userBoundary.adjust(sheaf, position);
                Page page = sheaf.getPage(adjusted, Page.class, new Page());
                synchronized (page.getRawPage())
                {
                    if (page.getRawPage().getByteBuffer().getInt(0) < 0)
                    {
                        BlockPage blocks = sheaf.getPage(adjusted,
                                BlockPage.class, new BlockPage());
                        if (alignRemaining(blocks.getRemaining()) != alignmentIndex
                                * alignment)
                        {
                            break;
                        }
                    }
                }
            }
        }

        return position;
    }

    /**
     * Clear the table, removing all pages and ignores.
     */
    public void clear()
    {
        byRemainingPage = null;
    }
}
