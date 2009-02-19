package com.goodworkalan.pack.vacuum;

/**
 * An interface to obtain existing user pages with enough space remaining to
 * accommodate a given size of addiontional blocks.
 * 
 * @author Alan Gutierrez
 */
public interface ByRemaining
{
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
    public long bestFit(int blockSize);
}