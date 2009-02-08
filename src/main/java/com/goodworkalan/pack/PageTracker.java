package com.goodworkalan.pack;

import java.util.Set;

final class PageTracker
extends CompositeMoveTracker
{
    private final SetTracker trackedUserPages;

    private final SetTracker journalPages;
    
    private final SetTracker writeBlockPages;
    
    private final SetTracker allocBlockPages;
    
    public PageTracker()
    {
        add(this.trackedUserPages = new SetTracker());
        add(this.journalPages = new SetTracker());
        add(this.writeBlockPages = new SetTracker());
        add(this.allocBlockPages = new SetTracker());
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