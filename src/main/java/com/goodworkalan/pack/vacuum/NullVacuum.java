package com.goodworkalan.pack.vacuum;

import java.util.Set;


/**
 * An implementation of vacuum that does nothing to reorder the
 * <code>Pack</code> file. This is the default vacuum strategy. It is useful for
 * applications where the <code>Pack</code> is used to temporarily allocate
 * blobs of data larger than a single block. An example might be a web page
 * cache or a mail queue.
 * 
 * @author Alan Gutierrez
 */
public class NullVacuum implements Vacuum
{
    /**
     * Do nothing.
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
    }
}
