package com.goodworkalan.pack;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A set of the empty block pages that can be used for block allocation. If the
 * position is in the set of free interim pages, then it is empty and available
 * for allocations. This class is used to track both user block pages and
 * interim pages.
 * <p>
 * The {@link #reserve(long) reserve} method is used to prevent a page from
 * being returned to the free set. When a reserved page position is returned
 * to the set with the {@link #free(long) free} method, it will be held until
 * it is released by the {@link #release(long) release} method. 
 * <p>
 * FIXME Not queuing ignored positions.
 * 
 * @author Alan Gutierrez
 */
final class FreeSet
implements Iterable<Long>
{
    private final SortedSet<Long> positions;
    
    private final SortedSet<Long> ignore;
    
    public FreeSet()
    {
        this.positions = new TreeSet<Long>();
        this.ignore = new TreeSet<Long>();
    }
    
    public synchronized int size()
    {
        return positions.size();
    }
    
    public Iterator<Long> iterator()
    {
        return positions.iterator();
    }
    
    /**
     * Remove the interim page from the set of free interim pages if the
     * page is in the set of free interim pages. Returns true if the page
     * was in the set of free interim pages.
     *
     * @param position The position of the interim free page.
     */
    public synchronized boolean reserve(long position)
    {
        if (positions.remove(position))
        {
            return true;
        }
        ignore.add(position);
        return false;
    }
    
    public synchronized void release(Set<Long> setToRelease)
    {
        ignore.removeAll(setToRelease);
    }
    
    public synchronized long allocate()
    {
        if (positions.size() != 0)
        {
            long position = positions.first();
            positions.remove(position);
            return position;
        }
        return 0L;
    }
    
    public synchronized void free(Set<Long> setOfPositions)
    {
        for (long position : setOfPositions)
        {
            free(position);
        }
    }

    public synchronized void free(long position)
    {
        if (!ignore.contains(position))
        {
            positions.add(position);
        }
    }
}