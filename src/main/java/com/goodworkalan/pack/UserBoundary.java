package com.goodworkalan.pack;

import java.util.HashSet;
import java.util.Set;

import com.goodworkalan.sheaf.Page;
import com.goodworkalan.sheaf.Sheaf;

public class UserBoundary
{
    /** The size of a page in the Pack.  */
    private final int pageSize;
    
    /** The position of the boundary.  */
    private long position;
    
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
    public UserBoundary(int pageSize, long position)
    {
        this.pageSize = pageSize;
        this.position = position;
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
    
    public int getPageSize()
    {
        return pageSize;
    }
    
    /**
     * Increment the boundary position by one page.
     */
    public void increment()
    {
        position += pageSize;
    }
    
    /**
     * Decrement the boundary position by one page.
     */
    public void decrement()
    {
        position -= pageSize;
    }

    /**
     * Return a file position based on the given file position adjusted by page
     * moves stored in the bouquets move map.
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
        int offset = (int) (position % getPageSize());
        position = position - offset;
        if (position < getPosition())
        {
            position = get(sheaf, position);
        }
        return position + offset;
    }

    public Set<Long> adjust(Sheaf sheaf, Set<Long> positions)
    {
        Set<Long> adjusted = new HashSet<Long>();
        for (long position : positions)
        {
            adjusted.add(adjust(sheaf, position));
        }
        return adjusted;
    }
    
    private long get(Sheaf sheaf, long position)
    {
        AddressPage addresses = sheaf.getPage(position, AddressPage.class, new AddressPage());
        return addresses.dereference(0);
    }

    /**
     * Dereferences the page referenced by the address, adjusting the file
     * position of the page according the list of user move latches.
     * 
     * @param address
     *            The block address.
     * @param userMoveLatches
     *            A list of the move latches that guarded the most recent user
     *            page moves.
     * @return The user block page.
     */
    public BlockPage dereference(Sheaf sheaf, long address)
    {
        // Get the address page.
        AddressPage addresses = sheaf.getPage(address, AddressPage.class, new AddressPage());
    
        // Assert that address is not a free address.
        long position = addresses.dereference(address);
        if (position == 0L || position == Long.MAX_VALUE)
        {
            throw new PackException(Pack.ERROR_FREED_ADDRESS);
        }
        
        while (position < getPosition())
        {
            position = get(sheaf, position);
        }
    
        return sheaf.getPage(position, BlockPage.class, new BlockPage());
    }
    
    public <P extends Page> P load(Sheaf sheaf, long position, Class<P> pageClass, P page)
    {
        while (position < getPosition())
        {
            position = get(sheaf, position);
        }
        
        return sheaf.getPage(position, pageClass, page);
    }
}
