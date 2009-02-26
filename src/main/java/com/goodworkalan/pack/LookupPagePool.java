package com.goodworkalan.pack;

import java.util.LinkedList;
import java.util.ListIterator;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Manages a file structure that stores file position values for lookup.
 * <p>
 * The file structure divides a page in to blocks of long values. The blocks of
 * long values are linked together to form a doubly linked list. The first two
 * long values in the block are used to store a previous and next page reference
 * to the other blocks in the doubly linked list. The remaining long values in
 * the block store the page position values as a stored array of values. The
 * empty array elements store zero values. The length of the array of values is
 * determined by searching for the first zero/empty element.
 * <p>
 * The block sizes are determined by a list of block sizes that contains one or
 * more block sizes. When the page pool allocates the first block in the linked
 * list of blocks, it uses the last size in the list of block sizes. When that
 * block is full and it allocates a second block, it uses the second to last
 * block size. When that block is full it allocates a third block using the
 * third to last size and so on, until the first block size is reached.
 * Thereafter all blocks allocated will be of the first block size.
 * <p>
 * For the primary application of the lookup page pool, the by remaining table,
 * this allows a small lookup table to use few pages. The lookup pools for 
 * each aligned by remaining value can allocate blocks from a common lookup page
 * for the given block size.
 * <p>
 * Lookup blocks are allocated from a lookup page. Different lookup page pools
 * can share lookup pages, so long as the lookup page pools share the same range
 * of block sizes.
 * 
 * @author Alan Gutierrez
 */
public class LookupPagePool
{
    /** The page manager. */
    private final Sheaf sheaf;
    
    /** The boundary between the address pages and the user pages. */
    private final AddressBoundary addressBoundary;
    
    /** The interim page pool. */
    private InterimPagePool interimPagePool; 
    
    /** The strategy to read and write head references to linked list. */ 
    private final LookupBlockPositionIO lookupBlockPositionIO;
    
    // TODO Document.
    private final LookupPagePositionIO lookupPagePositionIO;

    /** A descending list of the slot sizes. */
    private final LinkedList<Integer> blockSizes;

    /**
     * Create a slot page pool with the given descending list of slot sizes.
     * <p>
     * The first slot size is the largest size of a slot, a page where all the
     * non-header bytes are slot positions. The slot sizes can then descend to a
     * minimum slot size.
     * 
     * @param blockSizes
     *           A descending list of the block sizes.
     */
    public LookupPagePool(Sheaf sheaf, AddressBoundary userBoundary, InterimPagePool interimPagePool, LookupPagePositionIO lookupPagePositionIO, LookupBlockPositionIO lookupBlockPositionIO)
    {
        this.sheaf = sheaf;
        this.addressBoundary = userBoundary;
        this.interimPagePool = interimPagePool;
        this.lookupPagePositionIO = lookupPagePositionIO;
        this.lookupBlockPositionIO = lookupBlockPositionIO;
        this.blockSizes = new LinkedList<Integer>(lookupPagePositionIO.getBlockSizes());
    }

    /**
     * Get the block size of the next block to link to a linked list of sorted
     * blocks based on the lock size of the given lookup page.
     * 
     * @param lookupPage
     *            The lookup page.
     * @return The block size to allocate for the next page in the linked list
     *         of sorted blocks.
     */
    
    private int getNextBlockSizeIndex(LookupPage lookupPage)
    {
        int blockSize = 0;
        ListIterator<Integer> sizes = blockSizes.listIterator();
        do
        {
            blockSize = sizes.next();
        }
        while (blockSize != lookupPage.getBlockSize());
        return sizes.previousIndex() == -1 ? 0 : sizes.previousIndex();
    }

    /**
     * Initiate a ..
     * 
     * @return A new slot position.
     */
    private void newLookupPage(int blockSizeIndex, DirtyPageSet dirtyPages)
    {
        LookupPage setPage = interimPagePool.newInterimPage(sheaf, LookupPage.class, new LookupPage(), dirtyPages, true);
        setPage.setBlockSize(blockSizes.get(blockSizeIndex), dirtyPages);
        lookupPagePositionIO.write(blockSizeIndex, setPage.getRawPage().getPosition(), dirtyPages);
    }

    /**
     * Allocate a new block of the block size given by the block size index. The
     * given previous block position will be linked to the new slot.
     * 
     * @param blockSizeIndex
     *            The index of the block size.
     * @param previous
     *            The previous block position.
     * @return The position of the new block.
     */
    private void newLookupBlock(int blockSizeIndex, long previous, DirtyPageSet dirtyPages)
    {
        long allocPagePosition = lookupPagePositionIO.read(blockSizeIndex);
        if (allocPagePosition == 0L)
        {
            newLookupPage(blockSizes.getLast(), dirtyPages);
            allocPagePosition = lookupPagePositionIO.read(blockSizeIndex);
        }
        LookupPage lookupPage = addressBoundary.load(sheaf, allocPagePosition, LookupPage.class, new LookupPage());
        long blockPosition = lookupPage.allocateBlock(previous, dirtyPages);
        if (blockPosition == 0L)
        {
            newLookupPage(getNextBlockSizeIndex(lookupPage), dirtyPages);
            allocPagePosition = lookupPagePositionIO.read(blockSizeIndex); 
            LookupPage newLookupPage = addressBoundary.load(sheaf, allocPagePosition, LookupPage.class, new LookupPage());
            long newBlockPosition = newLookupPage.allocateBlock(previous, dirtyPages);
            lookupPage.setNext(blockPosition, newBlockPosition, dirtyPages);
            blockPosition = newBlockPosition;
        }
        lookupBlockPositionIO.write(blockPosition, dirtyPages);
    }

    /**
     * Return a file position based on the given file position adjusting the
     * position if it references a page that was moved to a new position to
     * create an address page in the address page region at the start of the
     * file.
     * 
     * @param position
     *            The file position.
     * @return The file position adjusted for any address page creation moves.
     */
    private long adjust(long position)
    {
        if (position != Long.MIN_VALUE && position != 0L)
        {
            position = addressBoundary.adjust(sheaf, position);
        }
        return position;
    }

    /**
     * Add the given value to a page in the lookup page pool. The lookup page
     * pool will not check for duplicates, so the given value may exist twice in
     * the lookup pages.
     * 
     * @param value
     *            The value to add.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void add(long value, DirtyPageSet dirtyPages)
    {
        long position = lookupBlockPositionIO.read();
        if (position == 0L)
        {
            newLookupBlock(blockSizes.getLast(), Long.MIN_VALUE, dirtyPages);
            position = lookupBlockPositionIO.read();
        }
        LookupPage lookupPage = addressBoundary.load(sheaf, position, LookupPage.class, new LookupPage());
        if (!lookupPage.add(position, value, false, dirtyPages))
        {
            newLookupBlock(getNextBlockSizeIndex(lookupPage), lookupPage.getRawPage().getPosition(), dirtyPages);
            position = lookupBlockPositionIO.read(); 
            lookupPage.add(position, value, false, dirtyPages);
        }
    }

    /**
     * Remove the given value from the lookup page pool. The method will remove
     * a single occurrence of the given value. If the value is duplicated in the
     * lookup page pool, it will still exist in the lookup page pool.
     * 
     * @param value
     *            The value to remove.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void remove(long value, DirtyPageSet dirtyPages)
    {
        long firstBlock = adjust(lookupBlockPositionIO.read());
        long block = firstBlock;
        if (block == 0L)
        {
            block = Long.MIN_VALUE;
        }
        boolean removed = false;
        while (!removed && block != Long.MIN_VALUE)
        {
            removed = true;
            // Start with head slot of the linked list of slots.
            LookupPage lookupPage = sheaf.getPage(adjust(block), LookupPage.class, new LookupPage());
            if (lookupPage.remove(block, value, dirtyPages))
            {
                if (block == firstBlock)
                {
                    lookupPage.compact(block, dirtyPages);
                }
                else
                {
                    LookupPage firstLookupPage = sheaf.getPage(adjust(firstBlock), LookupPage.class, new LookupPage());
                    long replace = firstLookupPage.remove(firstBlock, dirtyPages);
                    if (replace == 0L)
                    {
                        long lastBlock = adjust(firstLookupPage.getPrevious(firstBlock));
                        LookupPage lastBlockPage = sheaf.getPage(adjust(lastBlock), LookupPage.class, new LookupPage());
                        lastBlockPage.setNext(lastBlock, Long.MIN_VALUE, dirtyPages);
                        lookupBlockPositionIO.write(lastBlock, dirtyPages);
                        if (lastBlock == block)
                        {
                            lookupPage.compact(block, dirtyPages);
                        }
                        else
                        {
                            replace = lastBlockPage.remove(firstBlock, dirtyPages);
                            lookupPage.add(block, replace, true, dirtyPages);
                        }
                    }
                    else
                    {
                        lookupPage.add(block, replace, true, dirtyPages);
                    }
                }
            }
            block = adjust(lookupPage.getPrevious(block));
        }
    }

    /**
     * Remove and return a value form the page pool. Will return the first value
     * from the last block page in the page pool. If the last block page is
     * empty, it will adjust the linked list of block pages so that the last
     * block page is the previous block page.
     * 
     * @param dirtyPages
     *            The dirty page set.
     * @return A value from the page pool or zero of the page pool is empty.
     */
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
            long block = adjust(lookupBlockPositionIO.read());
            if (block == 0L)
            {
                return 0L;
            }
            
            LookupPage lookupPage = addressBoundary.load(sheaf, block, LookupPage.class, new LookupPage());
            long position = lookupPage.remove(block, dirtyPages);
            if (position != 0L)
            {
                return position;
            }

            // Get the previous slot.
            long previous = adjust(lookupPage.getPrevious(block));

            // Use the slot to allocate positions in the future.
            lookupBlockPositionIO.write(previous, dirtyPages);

            // Determine the slot size index of the slot page.
            ListIterator<Integer> sizes = blockSizes.listIterator();
            while (sizes.next() != lookupPage.getBlockSize())
            {
            }
            int blockSizeIndex = sizes.previousIndex() + 1;

            // Get the allocation slot page for the slot size.
            long alloc = adjust(lookupPagePositionIO.read(blockSizeIndex));

            // If the allocation slot page is not our previous slot page, we
            // move a slot from the allocation slot page onto the previous
            // slot page, so that all of the slots in the previous slot page
            // remain full and only the alloc slot page has empty slots.
            if (alloc != block)
            {
                LookupPage allocBlockPage = addressBoundary.load(sheaf, alloc, LookupPage.class, new LookupPage());
                
                // Remove the values for any slot in the alloc page.
                long[] values = allocBlockPage.removeBlock(dirtyPages);
                if (values != null)
                {
                    // Copy the values form the allocation slot to the now
                    // empty slot in the previous page.
        
                    // Set the previous and next pointers.
                    lookupPage.setPrevious(block, adjust(values[0]), dirtyPages);
                    lookupPage.setNext(block, adjust(values[1]), dirtyPages);
        
                    // Add the slot values. Remember that the slot is empty,
                    // so we don't need to explicitly zero the slot.
                    for (int i = 2; i < values.length; i++)
                    {
                        lookupPage.add(block, adjust(values[i]), false, dirtyPages);
                    }
        
                    // If there is a previous slot, set its next slot
                    // reference to this slot.
                    if (lookupPage.getPrevious(block) != Long.MIN_VALUE)
                    {
                        LookupPage lastLookupPage = addressBoundary.load(sheaf, lookupPage.getPrevious(block), LookupPage.class, new LookupPage());
                        lastLookupPage.setNext(lookupPage.getPrevious(block), block, dirtyPages);
                    }
        
                    // If there is a next slot, set its previous slot
                    // reference to this slot.
                    if (lookupPage.getNext(block) != Long.MIN_VALUE)
                    {
                        LookupPage nextLookupPage = addressBoundary.load(sheaf, lookupPage.getNext(block), LookupPage.class, new LookupPage());
                        nextLookupPage.setPrevious(lookupPage.getNext(block), block, dirtyPages);
                    }
                }
                else
                {
                    // The alloc slot page is empty, so lets free it and
                    // make the previous slot page the allocation slot page.
                    lookupPagePositionIO.write(blockSizeIndex, previous, dirtyPages);
                    interimPagePool.free(sheaf, alloc);
                }
            }
        }
    }
 }
