package com.goodworkalan.pack;

import java.util.SortedSet;
import java.util.TreeSet;

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
    /** The page manager. */
    private final Sheaf sheaf;
    
    /**
     * The maximum number of user pages that should be on the user side of the
     * user page boundary.
     */
    private int maxUserPoolSize;

    /**
     * Pages before this boundary can be used as long term pages, pages after
     * this boundary should be used as interim pages.
     */
    private long userToInterimBoundary;
    
    /** The set of free interim pages. */
    private final SortedSet<Long> freeInterimPages;

    /**
     * Create an empty interim page pool.
     * 
     * @param sheaf
     *            The page manager.
     */
    public InterimPagePool(Sheaf sheaf)
    {
        this.sheaf = sheaf;
        this.maxUserPoolSize = 24;
        this.freeInterimPages = new TreeSet<Long>();
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
    public long newBlankInterimPage(boolean durable)
    {
        long position = 0L;
        synchronized (freeInterimPages)
        {
            int size = freeInterimPages.size();
            if (size != 0)
            {
                if (durable)
                {
                    position = freeInterimPages.first();
                    if (position > userToInterimBoundary)
                    {
                        userToInterimBoundary = position;
                    }
                    freeInterimPages.remove(position);
                }
                else
                {                     
                    position = freeInterimPages.last();
                    if (position < userToInterimBoundary)
                    {
                        if (size >= maxUserPoolSize)
                        {
                            position = 0L;
                        }
                        else
                        {
                            userToInterimBoundary -= (sheaf.getPageSize() * (size - maxUserPoolSize));
                            if (position < userToInterimBoundary)
                            {
                                position = 0L;
                            }
                            else
                            {
                                freeInterimPages.remove(position);
                            }
                        }
                    }
                    else
                    {
                        freeInterimPages.remove(position);
                    }
                }
            }
        }
        if (position == 0L)
        {
            position = sheaf.extend();
            if (durable)
            {
                long nextPage = position + sheaf.getPageSize();
                synchronized (freeInterimPages)
                {
                    if (userToInterimBoundary < nextPage)
                    {
                        userToInterimBoundary = nextPage;
                    }
                }
            }
        }
        return position;
    }

    /**
     * Free the page at the given position, returning it to the interim page
     * pool for use by any mutator or vacuum.
     * 
     * @param position
     *            The page position to free.
     */
    public void free(long position)
    {
        sheaf.free(position);
        synchronized (freeInterimPages)
        {
            freeInterimPages.add(position);
        }
    }

    /**
     * Remove the page at the given page position, preventing it from being used
     * as an interim page by any mutator. Returns true if the page is in the
     * interim page pool.
     * <p>
     * This method is called by the address page pool to remove a page that is
     * about to be turned into an address page if it exists in the interim page
     * pool.
     * 
     * @param position
     *            The page position to free.
     * @return True if the page position is in the interim page pool.
     */
    public boolean remove(long position)
    {
        synchronized (freeInterimPages)
        {
            return freeInterimPages.remove(position);
        }
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
    public <T extends Page> T newInterimPage(Class<T> pageClass, T page, DirtyPageSet dirtyPages, boolean durable)
    {
        // We pull from the end of the interim space to take pressure of of
        // the durable pages, which are more than likely multiply in number
        // and move interim pages out of the way. We could change the order
        // of the interim page set, so that we choose free interim pages
        // from the front of the interim page space, if we want to rewind
        // the interim page space and shrink the file more frequently.
    
        long position = newBlankInterimPage(durable);
    
        // If we do not have a free interim page available, we will obtain
        // create one out of the wilderness.
    
        if (position == 0L)
        {
            position = sheaf.extend();
        }
    
        return sheaf.setPage(position, pageClass, page, dirtyPages);
    }
}
