package com.goodworkalan.pack.vacuum;

import java.util.Set;


/**
 * A strategy for relocating blocks, combining new allocations with existing
 * allocations, reclaiming space lost in pages due to block frees.
 * 
 * @author Alan Gutierrez
 */
public interface Vacuum
{
    /**
     * Vacuum a pack file by merging and moving block pages.
     * 
     * @param moveRecorder
     *            Used to record the moves prescribed by this strategy.
     * @param byRemaining
     *            A table of user block pages ordered by space remaining.
     * @param allocatedBlockPages
     *            The block pages allocated since the last vacuum.
     * @param freedBlockPages
     *            The block pages with freed blocks followed by allocated blocks
     *            created by frees since the last vacuum.
     */
    public void vacuum(MoveRecorder moveRecorder, ByRemaining byRemaining, Set<Long> allocatedBlockPages, Set<Long> freedBlockPages);
}
