package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;

/**
 * An I/O strategy to write the page position of the first lookup block in a
 * linked list of lookup blocks for a lookup page pool.
 * 
 * @author Alan Gutierrez
 */
public interface LookupBlockPositionIO
{
    /**
     * Write the head value of the linked list of of lookup blocks. The dirty
     * page set is used to track any pages that are allocated or update by this
     * method.
     * 
     * @param position
     *            The page position of the first block in a linked list of
     *            lookup blocks.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void write(long position, DirtyPageSet dirtyPages);
    
    /**
     * Read the head value of the linked list of of lookup blocks.
     * 
     * @return The page position of the first block in a linked list of lookup
     *         blocks.
     */
    public long read();
}
