package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Manages a pool of free interim pages.
 * 
 * @author Alan Gutierrez
 */
class InterimPagePool
{
    /**
     * A sorted set of of free interim pages sorted in descending order so that
     * we can quickly obtain the last free interim page within interim page
     * space.
     * <p>
     * This set of free interim pages guards against overwrites by a simple
     * method. If the position is in the set of free interim pages, then it is
     * free, if not it is not free. System pages must be allocated while the
     * move lock is locked for reading, or locked for writing in the case of
     * removing free pages from the start of the interim page area when the user
     * area expands.
     * <p>
     * Question: Can't an interim page allocated from the set of free pages be
     * moved while we are first writing to it?
     * <p>
     * Answer: No, because the moving mutator will have to add the moves to the
     * move list before it can move the pages. Adding to move list requires an
     * exclusive lock on the move list.
     * <p>
     * Remember: Only one mutator can move pages in the interim area at a time.
     */
    private final FreeSet freeInterimPages;
    
    /**
     * Create an empty interim page pool.
     */
    public InterimPagePool()
    {
        this.freeInterimPages = new FreeSet();
    }

    /**
     * Return an interim page for use as a move destination.
     * <p>
     * Question: How do we ensure that free interim pages do not slip into the
     * user data page section? That is, how do we ensure that we're not moving
     * an interim page to a spot that also needs to move?
     * <p>
     * Simple. We gather all the pages that need to move first. Then we assign
     * blank pages only to the pages that are in use and need to move. See
     * <code>tryMove</code> for more discussion.
     * 
     * @return A blank position in the interim area that for use as the target
     *         of a move.
     */
    public long newBlankInterimPage(Sheaf sheaf)
    {
        long position = getFreeInterimPages().allocate();
        if (position == 0L)
        {
            position = sheaf.extend();
        }
        return position;
    }

    /**
     * Return the set of completely empty interim pages available for block
     * allocation. The set returned is a class that not only contains the set of
     * pages available, but will also prevent a page from being returned to the
     * set of free pages, if that page is in the midst of relocation.
     * <p>
     * A user page is used by one mutator commit at a time. Removing the page
     * from this table prevents it from being used by another commit.
     * <p>
     * Removing a page from this set, prevents it from being used as an interim
     * page for an allocation or write. Removing a page from the available pages
     * sets is the first step in relocating a page.
     *
     * @return The set of free user pages.
     */
    public FreeSet getFreeInterimPages()
    {
        return freeInterimPages;
    }

    /**
     * Allocate a new interim position that is initialized by the specified page
     * strategy.
     * <p>
     * This method can only be called from within one of the
     * <code>MoveList.mutate</code> methods. A page obtained from the set of
     * free interim pages will not be moved while the move list is locked
     * shared.
     * 
     * @param <T>
     *            The page strategy for the position.
     * @param page
     *            An instance of the page strategy that will initialize the page
     *            at the position.
     * @param dirtyPages
     *            A map of dirty pages.
     * @return A new interim page.
     */
    public <T extends Page> T newInterimPage(Sheaf pager, Class<T> pageClass, T page, DirtyPageSet dirtyPages)
    {
        // We pull from the end of the interim space to take pressure of of
        // the durable pages, which are more than likely multiply in number
        // and move interim pages out of the way. We could change the order
        // of the interim page set, so that we choose free interim pages
        // from the front of the interim page space, if we want to rewind
        // the interim page space and shrink the file more frequently.
    
        long position = getFreeInterimPages().allocate();
    
        // If we do not have a free interim page available, we will obtain
        // create one out of the wilderness.
    
        if (position == 0L)
        {
            position = pager.extend();
        }
    
        return pager.setPage(position, pageClass, page, dirtyPages, false);
    }
}
