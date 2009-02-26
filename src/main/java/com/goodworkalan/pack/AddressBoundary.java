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
    /** The size of a page in the Pack.  */
    private final int pageSize;
    
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
    public AddressBoundary(int pageSize, long position)
    {
        this.pageSize = pageSize;
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
        position += pageSize;
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
    public long adjust(Sheaf sheaf, long position)
    {
        int offset = (int) (position % pageSize);
        position = position - offset;
        if (position < getPosition())
        {
            position = getForwardReference(sheaf, position);
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
    public Set<Long> adjust(Sheaf sheaf, Set<Long> positions)
    {
        Set<Long> adjusted = new HashSet<Long>();
        for (long position : positions)
        {
            adjusted.add(adjust(sheaf, position));
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
    private long getForwardReference(Sheaf sheaf, long position)
    {
        AddressPage addresses = sheaf.getPage(position, AddressPage.class, new AddressPage());
        return addresses.dereference(0);
    }

    /**
     * Dereferences the block page referenced by the address, adjusting the
     * position for any page moves made to accommodate address pages.
     * 
     * @param sheaf
     *            The page manager.
     * @param address
     *            The block address.
     * @return The user block page.
     */
    public BlockPage dereference(Sheaf sheaf, long address)
    {
        for (;;)
        {
            // Get the address page.
            AddressPage addresses = sheaf.getPage(address, AddressPage.class, new AddressPage());
        
            // Assert that address is not a free address.
            long position = addresses.dereference(address);
            if (position == 0L || position == Long.MAX_VALUE)
            {
                throw new PackException(PackException.ERROR_FREED_ADDRESS);
            }
            
            while (position < getPosition())
            {
                position = getForwardReference(sheaf, position);
            }
            
            // FIXME What if blocks are moved, page is reclaimed, page is
            // reassigned, right here?
            Page page = sheaf.getPage(position, Page.class, new Page());
            synchronized (page.getRawPage())
            {
                if (page.getRawPage().getByteBuffer().get(0) < 0)
                {
                    return sheaf.getPage(position, BlockPage.class, new BlockPage());
                }
            }
        }
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
    public <P extends Page> P load(Sheaf sheaf, long position, Class<P> pageClass, P page)
    {
        while (position < getPosition())
        {
            position = getForwardReference(sheaf, position);
        }
        
        return sheaf.getPage(position, pageClass, page);
    }
}
