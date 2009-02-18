package com.goodworkalan.pack;

import java.util.Iterator;
import java.util.Set;

/**
 * A simple vacuum strategy that merges each newly allocated block page with 
 * the first existing user block page that best fits the new page.
 *   
 * @author Alan Gutierrez
 */
public class BestFitVacuum implements Vacuum
{
    // FIXME Comment.
    public void vacuum(MoveRecorder moveRecorder, ByRemainingTable byRemaining, Set<Long> allocatedBlockPages, Set<Long> freedBlockPages)
    {
        for (long position : freedBlockPages)
        {
            byRemaining.remove(position);
        }
        
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
