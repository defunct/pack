package com.goodworkalan.pack;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

// TODO Document.
public class SlotPagePool
{
    // TODO Document.
    private final Sheaf sheaf;
    
    // TODO Document.
    private final UserBoundary userBoundary;
    
    // TODO Document.
    private InterimPagePool interimPagePool; 
    
    // TODO Document.
    private final PositionIO positionIO;

    /** A descending list of the slot sizes. */
    private final LinkedList<Integer> slotSizes;

    /**
     * Create a slot page pool with the given descending list of slot sizes.
     * <p>
     * The first slot size is the largest size of a slot, a page where all the
     * non-header bytes are slot positions. The slot sizes can then descend to a
     * minimum slot size.
     * 
     * @param slotSizes
     *           A descending list of the slot sizes.
     */
    public SlotPagePool(Sheaf sheaf, UserBoundary userBoundary, InterimPagePool interimPagePool, PositionIO positionIO, List<Integer> slotSizes)
    {
        this.sheaf = sheaf;
        this.userBoundary = userBoundary;
        this.interimPagePool = interimPagePool;
        this.positionIO = positionIO;
        this.slotSizes = new LinkedList<Integer>(slotSizes);
    }

    /**
     * Initiate a ..
     * 
     * @return A new slot position.
     */
    private long newSlotList(int slotSizeIndex, DirtyPageSet dirtyPages)
    {
        SlotPage setPage = interimPagePool.newInterimPage(sheaf, SlotPage.class, new SlotPage(), dirtyPages, true);
        setPage.setSlotSize(slotSizes.get(slotSizeIndex), dirtyPages);
        positionIO.write(setPage.getRawPage().getPosition(), dirtyPages);
        return setPage.getRawPage().getPosition();
    }
    
    // TODO Document.
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
    private long newSlotPosition(int slotIndex, long previous, DirtyPageSet dirtyPages)
    {
        long allocateFrom = positionIO.readAlloc(slotIndex);
        if (allocateFrom == 0L)
        {
            allocateFrom = newSlotList(slotSizes.getLast(), dirtyPages);
        }
        SlotPage slotPage = userBoundary.load(sheaf, allocateFrom, SlotPage.class, new SlotPage());
        long slotPosition = slotPage.allocateSlot(previous, dirtyPages);
        if (slotPosition == 0L)
        {
            allocateFrom = newSlotList(getNextSlotIndex(slotPage), dirtyPages);
            SlotPage newSlotPage = userBoundary.load(sheaf, allocateFrom, SlotPage.class, new SlotPage());
            long newSlotPosition = newSlotPage.allocateSlot(previous, dirtyPages);
            slotPage.setNext(slotPosition, newSlotPosition, dirtyPages);
            slotPosition = newSlotPosition;
        }
        positionIO.writeAlloc(slotIndex, slotPosition, dirtyPages);
        return slotPosition;
    }

    // TODO Document.
    public long add(long position, DirtyPageSet dirtyPages)
    {
        long slotPosition = 0L;
        long allocateFrom = 0L;
        if (slotPosition == 0L)
        {
            slotPosition = newSlotPosition(slotSizes.getLast(), Long.MIN_VALUE, dirtyPages);
        }
        SlotPage slotPage = userBoundary.load(sheaf, allocateFrom, SlotPage.class, new SlotPage());
        if (!slotPage.add(allocateFrom, position, false, dirtyPages))
        {
            slotPosition = newSlotPosition(getNextSlotIndex(slotPage), slotPage.getRawPage().getPosition(), dirtyPages);
            slotPage.add(allocateFrom, position, false, dirtyPages);
        }
        return slotPosition;
    }
    
    // TODO Document.
    private long adjust(long position)
    {
        if (position != Long.MIN_VALUE && position != 0L)
        {
            position = userBoundary.adjust(sheaf, position);
        }
        return position;
    }
    
    // TODO Document.
    public boolean remove(long position, DirtyPageSet dirtyPages)
    {
        long firstSlot = adjust(positionIO.read());
        long slot = firstSlot;
        if (slot == 0L)
        {
            slot = Long.MIN_VALUE;
        }
        boolean removed = false;
        while (!removed && slot != Long.MIN_VALUE)
        {
            removed = true;
            // Start with head slot of the linked list of slots.
            SlotPage slotPage = sheaf.getPage(adjust(slot), SlotPage.class, new SlotPage());
            if (slotPage.remove(slot, position, dirtyPages))
            {
                if (slot == firstSlot)
                {
                    slotPage.compact(slot, dirtyPages);
                }
                else
                {
                    SlotPage firstSlotPage = sheaf.getPage(adjust(firstSlot), SlotPage.class, new SlotPage());
                    long replace = firstSlotPage.remove(firstSlot, dirtyPages);
                    if (replace == 0L)
                    {
                        long lastSlot = adjust(firstSlotPage.getPrevious(firstSlot));
                        SlotPage lastSlotPage = sheaf.getPage(adjust(lastSlot), SlotPage.class, new SlotPage());
                        lastSlotPage.setNext(lastSlot, Long.MIN_VALUE, dirtyPages);
                        positionIO.write(lastSlot, dirtyPages);
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
            }
            slot = adjust(slotPage.getPrevious(slot));
        }
        return removed;
    }
    
    // TODO Document.
    public long poll(DirtyPageSet dirtyPages)
    {
        // If there is no position, we need to go to the previous slot. We
        // also need to remove the current slot. When we do, we want to make
        // sure that there are no empty slots, except in the slot page used
        // to allocate slots. If the empty slot is not on the slot allocation
        // page, we are going to remove a slot from the slot allocation page
        // and copy it into the slot page to fill the gap.
        for (;;)
        {
            // Start with head slot of the linked list of slots.
            long slot = adjust(positionIO.read());
            if (slot == 0L)
            {
                return 0L;
            }
            
            SlotPage slotPage = userBoundary.load(sheaf, slot, SlotPage.class, new SlotPage());
            long position = slotPage.remove(slot, dirtyPages);
            if (position != 0L)
            {
                return position;
            }

            // Get the previous slot.
            long previous = adjust(slotPage.getPrevious(slot));

            // Use the slot to allocate positions in the future.
            positionIO.write(previous, dirtyPages);

            // Determine the slot size index of the slot page.
            ListIterator<Integer> sizes = slotSizes.listIterator();
            while (sizes.next() != slotPage.getSlotSize())
            {
            }
            int slotIndex = sizes.previousIndex() + 1;

            // Get the allocation slot page for the slot size.
            long alloc = adjust(positionIO.readAlloc(slotIndex));

            // If the allocation slot page is not our previous slot page, we
            // move a slot from the allocation slot page onto the previous
            // slot page, so that all of the slots in the previous slot page
            // remain full and only the alloc slot page has empty slots.
            if (alloc != slot)
            {
                SlotPage allocSlotPage = userBoundary.load(sheaf, alloc, SlotPage.class, new SlotPage());
                
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
                        SlotPage lastSlotPage = userBoundary.load(sheaf, slotPage.getPrevious(slot), SlotPage.class, new SlotPage());
                        lastSlotPage.setNext(slotPage.getPrevious(slot), slot, dirtyPages);
                    }
        
                    // If there is a next slot, set its previous slot
                    // reference to this slot.
                    if (slotPage.getNext(slot) != Long.MIN_VALUE)
                    {
                        SlotPage nextSlotPage = userBoundary.load(sheaf, slotPage.getNext(slot), SlotPage.class, new SlotPage());
                        nextSlotPage.setPrevious(slotPage.getNext(slot), slot, dirtyPages);
                    }
                }
                else
                {
                    // The alloc slot page is empty, so lets free it and
                    // make the previous slot page the allocation slot page.
                    positionIO.writeAlloc(slotIndex, previous, dirtyPages);
                    interimPagePool.free(sheaf, alloc);
                }
            }
        }
    }
 }
