package com.goodworkalan.pack.vacuum;

import java.util.Iterator;
import java.util.Set;


/**
 * A simple vacuum strategy that merges each newly allocated block page with 
 * the first existing user block page that best fits the new page if any.
 * <p>
 * A rather naive best fit vacuum algorithm.
 *   
 * @author Alan Gutierrez
 */
public class BestFitVacuum implements Vacuum
{
    /**
     * Vacuum a pack file by merging each newly allocated block page with the
     * first existing user block page that best fits the new page if any.
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
    public void vacuum(MoveRecorder moveRecorder, ByRemaining byRemaining, Set<Long> allocatedBlockPages, Set<Long> freedBlockPages)
    {
        int pageSize = moveRecorder.getPageSize();
        
        Iterator<Long> discontinuous = freedBlockPages.iterator();
        while (discontinuous.hasNext())
        {
            long position = discontinuous.next();
            long bestFit = byRemaining.bestFit(pageSize - moveRecorder.getBytesRemaining(position));
            if (bestFit != 0)
            {
                moveRecorder.move(position, bestFit);
                discontinuous.remove();
            }
        }
        
        for (long position : freedBlockPages)
        {
            moveRecorder.move(position);
        }
        
        Iterator<Long> allocated = allocatedBlockPages.iterator();
        while (allocated.hasNext())
        {
            long position = allocated.next();
            long bestFit = byRemaining.bestFit(pageSize - moveRecorder.getBytesRemaining(position));
            if (bestFit != 0)
            {
                moveRecorder.move(position, bestFit);
                allocated.remove();
            }
        }
    }
}
