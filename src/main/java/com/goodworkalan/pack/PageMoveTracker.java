package com.goodworkalan.pack;

import java.util.Set;

final class PageMoveTracker
extends CompositeMoveTracker
{
    private final SetMoveTracker trackedUserPages;

    private final SetMoveTracker journalPages;
    
    private final SetMoveTracker writeBlockPages;
    
    private final SetMoveTracker allocBlockPages;
    
    public PageMoveTracker()
    {
        add(this.trackedUserPages = new SetMoveTracker());
        add(this.journalPages = new SetMoveTracker());
        add(this.writeBlockPages = new SetMoveTracker());
        add(this.allocBlockPages = new SetMoveTracker());
    }
    
    public Set<Long> getTrackedUserPages()
    {
        return trackedUserPages;
    }
    
    public Set<Long> getJournalPages()
    {
        return journalPages;
    }

    /**
     * Return a set of interim block pages used to store blocks that represent a
     * write to an existing block in a user block page.
     * 
     * @return The set of interim block pages containing writes.
     */
    public Set<Long> getWriteBlockPages()
    {
        return writeBlockPages;
    }

    /**
     * Return a set of interim block pages used to store newly allocated blocks
     * that do not yet have a block in a user block page.
     * 
     * @return The set of interim block pages containing block allocations.
     */
    public Set<Long> getAllocBlockPages()
    {
        return allocBlockPages;
    }
}