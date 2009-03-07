package com.goodworkalan.pack;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.goodworkalan.sheaf.Page;
import com.goodworkalan.sheaf.Sheaf;

/**
 * The boundary between address pages and non-address pages. The boundary can
 * only be modified if the {@link Bouquet#getPageMoveLock() page move lock} of
 * the bouquet is held exclusively.
 * 
 * @author Alan Gutierrez
 */
class AddressBoundary
{
    /** The page manager. */
    private final Sheaf sheaf;
    
    /** The position of the boundary.  */
    private long position;
    
    /** A read/write lock to guard the address page to user boundary. */
    private final ReadWriteLock pageMoveLock;
    
    /**
     * Create a boundary tracker for pages of the given size that is set at the
     * given position.
     * 
     * @param pageSize
     *            The size of a page used to increment and decrement the
     *            position.
     * @param position
     *            The initial position.
     */
    public AddressBoundary(Sheaf sheaf, long position)
    {
        this.sheaf = sheaf;
        this.position = position;
        this.pageMoveLock = new ReentrantReadWriteLock();
    }
    
    /**
     * Get the read/write lock used to guard the user boundary. The read lock
     * of this read/write lock must be held by any operation attempting to
     * read a non-address page.
     * 
     * @return The read/write lock used to guard the user boundary.
     */
    public ReadWriteLock getPageMoveLock()
    {
        return pageMoveLock;
    }
    
    /**
     * Get the position of the boundary.
     * 
     * @return The position of the boundary.
     */
    public long getPosition()
    {
        return position;
    }
    
    /**
     * Increment the boundary position by one page.
     */
    public void increment()
    {
        position += sheaf.getPageSize();
    }

    /**
     * Return a file position based on the given file position adjusting the
     * position if it references a page that was moved to a new position to
     * create an address page in the address page region at the start of the
     * file.
     * <p>
     * The adjustment will account for offset into the page position. This is
     * necessary for next operations in journals, which may jump to any
     * operation in a journal, which may be at any location in a page.
     * 
     * @param position
     *            The file position to track.
     * @return The file position adjusted by the recorded page moves.
     */
    public long adjust(long position)
    {
        int offset = (int) (position % sheaf.getPageSize());
        position = position - offset;
        if (position < getPosition())
        {
            position = getForwardReference(position);
        }
        return position + offset;
    }

    /**
     * Create a set of page positions from the given set of positions adjusting
     * each position for any page moves made to accommodate address pages.
     * 
     * 
     * @param sheaf
     *            The page manager.
     * @param positions
     *            A set of page positions.
     * @return A set of page positions adjusted for page moves.
     */
    public Set<Long> adjust(Set<Long> positions)
    {
        Set<Long> adjusted = new HashSet<Long>();
        for (long position : positions)
        {
            adjusted.add(adjust(position));
        }
        return adjusted;
    }

    /**
     * Get the page position user page moved to create an address page for the
     * given user page position.
     * 
     * @param sheaf
     *            The page manager.
     * @param position
     *            The page position.
     * @return The page position the user page was moved to.
     */
    private long getForwardReference(long position)
    {
        AddressPage addresses = sheaf.getPage(position, AddressPage.class, new AddressPage());
        return addresses.dereference(0);
    }

    /**
     * Dereferences the block page referenced by the address, adjusting the
     * position for any page moves made to accommodate address pages. Returns an
     * object used to double check the reference after holding onto the monitor
     * for the raw page.
     * 
     * @param sheaf
     *            The page manager.
     * @param address
     *            The block address.
     * @return A dereference object used to double check the dereference.
     */
    public Dereference dereference(long address)
    {
        // Get the address page.
        AddressPage addresses = sheaf.getPage(address, AddressPage.class, new AddressPage());
    
        // Assert that address is not a free address.
        long initial = addresses.dereference(address);
        if (initial == 0L || initial == Long.MAX_VALUE)
        {
            throw new PackException(PackException.ERROR_FREED_ADDRESS);
        }
        
        // If the position is less than the address boundary, follow to
        // where the page has been moved.

        long position = initial;
        while (position < getPosition())
        {
            position = getForwardReference(position);
        }
        
        // Get the raw page with the base page class.
        Page page = sheaf.getPage(position, Page.class, new Page());
        return new Dereference(address, initial, page.getRawPage());
    }

    /**
     * Load a page of the given page class at the given position from the given
     * sheaf after adjusting the position for any page moves made to accommodate
     * address pages. Use the given page instance to initialize the page if it
     * is not already loaded.
     * 
     * @param <P>
     *            The page type.
     * @param sheaf
     *            The page manager.
     * @param position
     *            The page position.
     * @param pageClass
     *            The page type.
     * @param page
     *            A page instance used to load the page if it is no already in
     *            memory.
     * @return A page of the given page type mapped to the contents at the page
     *         position.
     */
    public <P extends Page> P load(long position, Class<P> pageClass, P page)
    {
        while (position < getPosition())
        {
            position = getForwardReference(position);
        }
        
        return sheaf.getPage(position, pageClass, page);
    }
}
