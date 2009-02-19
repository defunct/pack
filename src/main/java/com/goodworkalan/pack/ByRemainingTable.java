package com.goodworkalan.pack;

import java.util.LinkedList;
import java.util.ListIterator;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;

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
 * FIXME New problem. Free a block. Same block vacuumed, moved.
 * FIXME Why am I  supporting the notion of user pages? A feature too far? If
 * you want to use block management, you have to use a block.
 * FIXME Don't I have isolation now? If you have one writer, you can dirty
 * up pages, so long as no one else is writing, that dirt can be in isolation.
 * Here is where you get your dirty pages in strata.
 * FIXME You can solve exposing a raw empty page to the user later.
 * FIXME How about, you can have a raw empty page, but you cannot set the
 * first bit of the page. Instead of reserving the first byte or long, reserve
 * the first bit. Assert that it is not set before writing. Problem solved,
 * now return to implementing.
 * 
 * @author Alan Gutierrez
 */
public final class ByRemainingTable
{
    /** A descending list of the sizes of */
    private LinkedList<Integer> slotSizes;
    
    /** The bouquet of services. */
    private final Bouquet bouquet;

    /**
     * A page for storing by remaining pages grouped by alignment and slot pages
     * grouped by slot size.
     */
    private ByRemainingPage byRemainingPage;
    
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
    public ByRemainingTable(Bouquet bouquet, DirtyPageSet dirtyPages)
    {
        this.bouquet = bouquet;
        this.dirtyPages = new DirtyPageSet(16);
        this.slotSizes = getSlotSizes(bouquet.getSheaf().getPageSize(), bouquet.getAlignment());
    }
    
    // TODO Comment.
    public void load(long position)
    {
        if (byRemainingPage == null)
        {
            if (position == 0L)
            {
                byRemainingPage = bouquet.getInterimPagePool().newInterimPage(bouquet.getSheaf(), ByRemainingPage.class, new ByRemainingPage(), dirtyPages, false);
            }
            else
            {
            }
        }
    }
    
    // TODO Comment.
    private static int getSlotSize(int pageSize, int slotCount)
    {
        return ((Pack.INT_SIZE - pageSize) / Pack.LONG_SIZE) / slotCount;
    }
    
    // TODO Comment.
    private static LinkedList<Integer> getSlotSizes(int pageSize, int alignment)
    {
        LinkedList<Integer> slotSizes = new LinkedList<Integer>();
        int slotCount = 1;
        for (;;)
        {
            int slotSize = getSlotSize(pageSize, slotCount);
            if (slotSize < 8)
            {
                break;
            }
            slotSizes.add(slotSize);
            slotCount *= 2;
        }
        return slotSizes;
    }

    /**
     * Add the page position and amount of bytes remaining for block allocation
     * of the given block page to the table. If the amount of bytes remaining
     * for block allocation rounds down to zero, the page position is not added
     * to the table.
     * <p>
     * If the page position of the block has been reserved, the page will be
     * put into a waiting state until the page has been released, at which
     * point it will be added.
     * 
     * @param blocks
     *            The block page to add.
     */
    public void add(BlockPage blocks)
    {
        add(blocks.getRawPage().getPosition(), blocks.getRemaining());
    }

    /**
     * Allocate a new slot of the slot size given by the slot size index and
     * added to the linked list of slots of the given alignment index. The given
     * previous slot position will be linked to the new slot.
     * 
     * @param slotSizeIndex
     *            The index of the slot size.
     * @param alignmentIndex
     *            The index of the alignment.
     * @param previous
     *            The previous slot position.
     * @return The position of the new slot.
     */
    public long newSlotPosition(int slotSizeIndex, int alignmentIndex, long previous)
    {
        long allocateFrom = byRemainingPage.getAllocSlotPosition(slotSizeIndex);
        if (allocateFrom == 0L)
        {
            allocateFrom = newSlotList(slotSizes.getLast());
        }
        ByRemainingSlotPage slotPage = bouquet.getUserBoundary().load(bouquet.getSheaf(), allocateFrom, ByRemainingSlotPage.class, new ByRemainingSlotPage());
        long slotPosition = slotPage.allocateSlot(previous, dirtyPages);
        if (slotPosition == 0L)
        {
            allocateFrom = newSlotList(getNextSlotIndex(slotPage));
            ByRemainingSlotPage newSlotPage = bouquet.getUserBoundary().load(bouquet.getSheaf(), allocateFrom, ByRemainingSlotPage.class, new ByRemainingSlotPage());
            long newSlotPosition = newSlotPage.allocateSlot(previous, dirtyPages);
            slotPage.setNext(slotPosition, newSlotPosition, dirtyPages);
            slotPosition = newSlotPosition;
        }
        byRemainingPage.setSlotPosition(alignmentIndex, slotPosition, dirtyPages);
        return slotPosition;
    }

    /**
     * Initiate a ..
     * @return
     */
    public long newSlotList(int slotSizeIndex)
    {
        ByRemainingSlotPage setPage = bouquet.getInterimPagePool().newInterimPage(bouquet.getSheaf(), ByRemainingSlotPage.class, new ByRemainingSlotPage(), dirtyPages, true);
        setPage.setSlotSize(slotSizeIndex, dirtyPages);
        byRemainingPage.setAllocSlotPosition(slotSizeIndex, setPage.getRawPage().getPosition(), dirtyPages);
        return setPage.getRawPage().getPosition();
    }
    
    // TODO Comment.
    private int alignRemaining(int remaining)
    {
        int alignment = bouquet.getAlignment();
        return remaining / alignment * alignment;
    }
    
    private int getNextSlotIndex(ByRemainingSlotPage slotPage)
    {
        int slotSize = 0;
        ListIterator<Integer> sizes = slotSizes.listIterator();
        do
        {
            slotSize = sizes.next();
        }
        while (slotSize != slotPage.getSlotSize());
        return sizes.previousIndex() == -1 ? 0 : sizes.previousIndex();
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
        
        int alignment = bouquet.getAlignment();
        int aligned = remaining / alignment * alignment;
        if (aligned != 0)
        {
            int alignmentIndex = aligned / alignment;
            byRemainingPage.increment(alignmentIndex, dirtyPages);
            long setPosition = byRemainingPage.getSlotPosition(alignmentIndex);
            long allocateFrom = 0L;
            if (setPosition == 0L)
            {
                setPosition = newSlotPosition(slotSizes.size() - 1, alignmentIndex, Long.MIN_VALUE);
            }
            ByRemainingSlotPage setPage = bouquet.getUserBoundary().load(bouquet.getSheaf(), allocateFrom, ByRemainingSlotPage.class, new ByRemainingSlotPage());
            if (!setPage.add(allocateFrom, position, false, dirtyPages))
            {
                setPosition = newSlotPosition(getNextSlotIndex(setPage), alignmentIndex, setPage.getRawPage().getPosition());
                setPage.add(allocateFrom, position, false, dirtyPages);
           }
        }
    }

    /**
     * Remove the given page position from the table.
     * <p>
     * Returns the amount of blocks remaining rounded down to the nearest
     * alignment. This is used by the {@link ByRemainingTableMoveTracker} to relocate
     * the page and amount remaining in the table, when the page moves.
     * 
     * @param position
     *            The page position.
     */
    public void remove(long position, int remaining)
    {
        int alignment = bouquet.getAlignment();
        int aligned = remaining / alignment * alignment;
        int alignmentIndex = alignment / aligned;
        byRemainingPage.decrement(alignmentIndex, dirtyPages);
        for (;;)
        {
            // Start with head slot of the linked list of slots.
            long slot = adjust(byRemainingPage.getSlotPosition(alignmentIndex));
            ByRemainingSlotPage slotPage = getSlotPage(slot);
            if (slotPage.remove(slot, position, dirtyPages))
            {
                
            }
            position = adjust(slotPage.remove(slot, dirtyPages));
        }
    }
    
    // TODO Comment.
    private int alignmentIndex(int aligned)
    {
        int alignment = bouquet.getAlignment();
        int pageSize = bouquet.getSheaf().getPageSize();

        for (int i = aligned / alignment; i < pageSize / alignment; i++)
        {
            if (byRemainingPage.getSizeCount(i) != 0)
            {
                return i;
            }
        }
        
        return 0;
    }
    
    // TODO Comment.
    private ByRemainingSlotPage getSlotPage(long position)
    {
        return bouquet.getUserBoundary().load(bouquet.getSheaf(), position, ByRemainingSlotPage.class, new ByRemainingSlotPage());
    }
    
    // TODO Comment.
    private long adjust(long position)
    {
        if (position != Long.MIN_VALUE && position != 0L)
        {
            position = bouquet.getUserBoundary().adjust(bouquet.getSheaf(), position);
        }
        return position;
    }

    /**
     * Return the page with the least amount of bytes remaining for allocation
     * that will fit the full block size. The block given block size must
     * include the block header.
     * <p>
     * The method will ascend the table looking at the slots for each remaining
     * size going form smallest to largest and returning the first to fit the
     * block, or null if no page can fit the block.
     * 
     * @param blockSize
     *            The block size including the block header.
     * @return A size object containing the a page that will fit the block or
     *         null if none exists.
     */
    public long bestFit(int blockSize)
    {
        // Get the page size, alignment and aligned block size.
        int alignment = bouquet.getAlignment();
        int aligned = ( blockSize + alignment - 1 ) / alignment * alignment;

        // Return zero there is no chance of a fit.
        if (aligned > bouquet.getPack().getMaximumBlockSize() - alignment)
        {
            return 0L;
        }
    
        // Return zero if there are no pages that can fit.
        int alignmentIndex = alignmentIndex(aligned);
        if (alignmentIndex == 0)
        {
            return 0L;
        }

        long position = 0L;
        for (;;)
        {
            // Start with head slot of the linked list of slots.
            long slot = adjust(byRemainingPage.getSlotPosition(alignmentIndex));
            ByRemainingSlotPage slotPage = getSlotPage(slot);
            position = adjust(slotPage.remove(slot, dirtyPages));

            // If there is no position, we need to go to the previous slot. We
            // also need to remove the current slot. When we do, we want to make
            // sure that there are no empty slots, except in the slot page used
            // to allocate slots. If the empty slot is not on the slot allocation
            // page, we are going to remove a slot from the slot allocation page
            // and copy it into the slot page to fill the gap.
            if (position == 0L)
            {
                // Get the previous slot.
                long previous = adjust(slotPage.getPrevious(slot));

                // Use the slot to allocate positions in the future.
                byRemainingPage.setSlotPosition(alignmentIndex, previous, dirtyPages);

                // Determine the slot size index of the slot page.
                ListIterator<Integer> sizes = slotSizes.listIterator();
                while (sizes.next() != slotPage.getSlotSize())
                {
                }
                int slotIndex = sizes.previousIndex() + 1;

                // Get the allocation slot page for the slot size.
                long alloc = adjust(byRemainingPage.getAllocSlotPosition(slotIndex));

                // If the allocation slot page is not our previous slot page, we
                // move a slot from the allocation slot page onto the previous
                // slot page, so that all of the slots in the previous slot page
                // remain full and only the alloc slot page has empty slots.
                if (alloc != slot)
                {
                    ByRemainingSlotPage allocSlotPage = getSlotPage(alloc);

                    // Remove the values for any slot in the alloc page.
                    long[] values = allocSlotPage.removeSlot(dirtyPages);
                    if (values != null)
                    {
                        // Copy the values form the allocation slot to the now
                        // empty slot in the previous page.

                        // Set the previous and next pointers.
                        slotPage.setPrevious(slot, adjust(values[0]), dirtyPages);
                        slotPage.setNext(slot, adjust(values[1]), dirtyPages);

                        // Add the slot values. Remember that the slot is empty,
                        // so we don't need to explicitly zero the slot.
                        for (int i = 2; i < values.length; i++)
                        {
                            slotPage.add(slot, adjust(values[i]), false, dirtyPages);
                        }

                        // If there is a previous slot, set its next slot
                        // reference to this slot.
                        if (slotPage.getPrevious(slot) != Long.MIN_VALUE)
                        {
                            ByRemainingSlotPage lastSlotPage = getSlotPage(slotPage.getPrevious(slot));
                            lastSlotPage.setNext(slotPage.getPrevious(slot), slot, dirtyPages);
                        }

                        // If there is a next slot, set its previous slot
                        // reference to this slot.
                        if (slotPage.getNext(slot) != Long.MIN_VALUE)
                        {
                            ByRemainingSlotPage nextSlotPage = getSlotPage(slotPage.getNext(slot));
                            nextSlotPage.setPrevious(slotPage.getNext(slot), slot, dirtyPages);
                        }
                    }
                    else
                    {
                        // The alloc slot page is empty, so lets free it and
                        // make the previous slot page the allocation slot page.
                        byRemainingPage.setAllocSlotPosition(slotIndex, previous, dirtyPages);
                        bouquet.getInterimPagePool().free(alloc);
                    }
                }
                else
                {
                    Page page = bouquet.getSheaf().getPage(position, Page.class, new Page());
                    synchronized (page.getRawPage())
                    {
                        if (page.getRawPage().getByteBuffer().getInt(0) < 0)
                        {
                            BlockPage blocks = bouquet.getSheaf().getPage(position, BlockPage.class, new BlockPage());
                            if (alignRemaining(blocks.getRemaining()) == alignmentIndex * alignment)
                            {
                                break;
                            }
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
