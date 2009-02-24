package com.goodworkalan.pack;

import java.util.LinkedList;
import java.util.ListIterator;

import com.goodworkalan.pack.vacuum.ByRemaining;
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
final class ByRemainingTable implements ByRemaining
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
        this.dirtyPages = new DirtyPageSet();
        this.slotSizes = getSlotSizes(bouquet.getSheaf().getPageSize(), bouquet.getHeader().getAlignment());
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
        SlotPage slotPage = bouquet.getUserBoundary().load(bouquet.getSheaf(), allocateFrom, SlotPage.class, new SlotPage());
        long slotPosition = slotPage.allocateSlot(previous, dirtyPages);
        if (slotPosition == 0L)
        {
            allocateFrom = newSlotList(getNextSlotIndex(slotPage));
            SlotPage newSlotPage = bouquet.getUserBoundary().load(bouquet.getSheaf(), allocateFrom, SlotPage.class, new SlotPage());
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
        SlotPage setPage = bouquet.getInterimPagePool().newInterimPage(bouquet.getSheaf(), SlotPage.class, new SlotPage(), dirtyPages, true);
        setPage.setSlotSize(slotSizeIndex, dirtyPages);
        byRemainingPage.setAllocSlotPosition(slotSizeIndex, setPage.getRawPage().getPosition(), dirtyPages);
        return setPage.getRawPage().getPosition();
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
        int alignment = bouquet.getHeader().getAlignment();
        return remaining / alignment * alignment;
    }
    
    private int getNextSlotIndex(SlotPage slotPage)
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
        
        int alignment = bouquet.getHeader().getAlignment();
        int aligned = remaining / alignment * alignment;
        if (aligned != 0)
        {
            int alignmentIndex = aligned / alignment;
            long slotPosition = byRemainingPage.getSlotPosition(alignmentIndex);
            long allocateFrom = 0L;
            if (slotPosition == 0L)
            {
                slotPosition = newSlotPosition(slotSizes.size() - 1, alignmentIndex, Long.MIN_VALUE);
            }
            SlotPage slotPage = bouquet.getUserBoundary().load(bouquet.getSheaf(), allocateFrom, SlotPage.class, new SlotPage());
            if (!slotPage.add(allocateFrom, position, false, dirtyPages))
            {
                slotPosition = newSlotPosition(getNextSlotIndex(slotPage), alignmentIndex, slotPage.getRawPage().getPosition());
                slotPage.add(allocateFrom, position, false, dirtyPages);
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
        int alignment = bouquet.getHeader().getAlignment();
        int aligned = remaining / alignment * alignment;
        int alignmentIndex = alignment / aligned;
        long firstSlot = adjust(byRemainingPage.getSlotPosition(alignmentIndex));
        long slot = firstSlot;
        while (slot != Long.MIN_VALUE)
        {
            // Start with head slot of the linked list of slots.
            SlotPage slotPage = getSlotPage(slot);
            if (slotPage.remove(slot, position, dirtyPages))
            {
                if (slot == firstSlot)
                {
                    slotPage.compact(slot, dirtyPages);
                }
                else
                {
                    SlotPage firstSlotPage = getSlotPage(firstSlot);
                    long replace = firstSlotPage.remove(firstSlot, dirtyPages);
                    if (replace == 0L)
                    {
                        long lastSlot = adjust(firstSlotPage.getPrevious(firstSlot));
                        SlotPage lastSlotPage = getSlotPage(lastSlot);
                        lastSlotPage.setNext(lastSlot, Long.MIN_VALUE, dirtyPages);
                        byRemainingPage.setSlotPosition(alignmentIndex, lastSlot, dirtyPages);
                        if (lastSlot == slot)
                        {
                            slotPage.compact(slot, dirtyPages);
                        }
                        else
                        {
                            replace = lastSlotPage.remove(firstSlot, dirtyPages);
                            slotPage.add(slot, replace, true, dirtyPages);
                        }
                    }
                    else
                    {
                        slotPage.add(slot, replace, true, dirtyPages);
                    }
                }
                break;
            }
            slot = adjust(slotPage.getPrevious(slot));
        }
    }
    
    // TODO Comment.
    private SlotPage getSlotPage(long position)
    {
        return bouquet.getUserBoundary().load(bouquet.getSheaf(), position, SlotPage.class, new SlotPage());
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
     * @see com.goodworkalan.pack.ByRemaining#bestFit(int)
     */
    public long bestFit(int blockSize)
    {
        // Get the page size, alignment and aligned block size.
        int alignment = bouquet.getHeader().getAlignment();
        int aligned = (blockSize + alignment - 1) / alignment * alignment;

        // Return zero there is no chance of a fit.
        if (aligned > bouquet.getPack().getMaximumBlockSize() - alignment)
        {
            return 0L;
        }
    
        // Return zero if there are no pages that can fit.
        long position = 0L;

        int pageSize = bouquet.getSheaf().getPageSize();
        for (int alignmentIndex = aligned / alignment; alignmentIndex < pageSize / alignment; alignmentIndex++)
        {
            for (;;)
            {
                // Start with head slot of the linked list of slots.
                long slot = adjust(byRemainingPage.getSlotPosition(alignmentIndex));
                SlotPage slotPage = getSlotPage(slot);
                position = slotPage.remove(slot, dirtyPages);
    
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
                        SlotPage allocSlotPage = getSlotPage(alloc);
    
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
                                SlotPage lastSlotPage = getSlotPage(slotPage.getPrevious(slot));
                                lastSlotPage.setNext(slotPage.getPrevious(slot), slot, dirtyPages);
                            }
    
                            // If there is a next slot, set its previous slot
                            // reference to this slot.
                            if (slotPage.getNext(slot) != Long.MIN_VALUE)
                            {
                                SlotPage nextSlotPage = getSlotPage(slotPage.getNext(slot));
                                nextSlotPage.setPrevious(slotPage.getNext(slot), slot, dirtyPages);
                            }
                        }
                        else
                        {
                            // The alloc slot page is empty, so lets free it and
                            // make the previous slot page the allocation slot page.
                            byRemainingPage.setAllocSlotPosition(slotIndex, previous, dirtyPages);
                            bouquet.getInterimPagePool().free(bouquet.getSheaf(), alloc);
                        }
                    }
                    else
                    {
                        Page page = bouquet.getSheaf().getPage(adjust(position), Page.class, new Page());
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
