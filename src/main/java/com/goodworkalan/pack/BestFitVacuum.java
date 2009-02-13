package com.goodworkalan.pack;

import java.util.Iterator;
import java.util.Set;

public class BestFitVacuum implements Vacuum
{
    public void vacuum(Mover mover, ByRemainingTable byRemaining, Set<Long> allocatedBlockPages, Set<Long> freedBlockPages)
    {
        for (long position : freedBlockPages)
        {
            byRemaining.remove(position);
        }
        
        int pageSize = mover.getSheaf().getPageSize();
        
        Iterator<Long> discontinuous = freedBlockPages.iterator();
        while (discontinuous.hasNext())
        {
            long position = discontinuous.next();
            BlockPage blocks = mover.getBlockPage(position);
            long bestFit = byRemaining.bestFit(pageSize - blocks.getRemaining());
            if (bestFit != 0)
            {
                mover.move(position, bestFit);
                discontinuous.remove();
            }
        }
        
        for (long position : freedBlockPages)
        {
            mover.move(position);
        }
        
        Iterator<Long> allocated = allocatedBlockPages.iterator();
        while (allocated.hasNext())
        {
            long position = allocated.next();
            BlockPage blocks = mover.getBlockPage(position);
            long bestFit = byRemaining.bestFit(pageSize - blocks.getRemaining());
            if (bestFit != 0)
            {
                mover.move(position, bestFit);
                allocated.remove();
            }
        }
    }
}
