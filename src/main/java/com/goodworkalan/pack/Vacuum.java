package com.goodworkalan.pack;

import java.util.Set;

/**
 * A strategy for relocating blocks, reclaiming space lost in pages due to block
 * frees, and generallly compacting the file.
 * 
 * @author Alan Gutierrez
 */
public interface Vacuum
{
    /**
     * Vacuum a pack file by merging and moving block pages.
     * 
     * @param mover
     *            A recorder for vacuum strategy.
     * @param byRemaining
     *            A table of user block pages ordered by space remaining.
     * @param allocatedBlockPages
     *            The block pages allocated since the last vacuum.
     * @param freedBlockPages
     *            The block pages with freed blocks followed by allocated blocks
     *            created by frees since the last vacuum.
     */
    public void vacuum(Mover mover, ByRemainingTable byRemaining, Set<Long> allocatedBlockPages, Set<Long> freedBlockPages);
}
