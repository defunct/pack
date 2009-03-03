package com.goodworkalan.pack;

import com.goodworkalan.sheaf.RawPage;
import com.goodworkalan.sheaf.Sheaf;

/**
 * A dereference of a block address that checks that the block has not moved
 * before returning the block page mapped to the address.
 * 
 * @author Alan Gutierrez
 */
public class Dereference
{
    /** The dereferenced address. */
    private final long address;
    
    /** The dereferenced page position. */
    private final long position;
    
    /** The raw page at the dereferenced page position. */
    private final RawPage rawPage;

    /**
     * Create a new dereference.
     * 
     * @param address
     *            The dereferenced address.
     * @param position
     *            The dereferenced page position.
     * @param rawPage
     *            The raw page at the dereferenced page position.
     */
    public Dereference(long address, long position, RawPage rawPage)
    {
        this.address = address;
        this.position = position;
        this.rawPage = rawPage;
    }

    /**
     * Get the dereferenced block page returning null if the block page has
     * moved since the block was dereferenced.
     * 
     * @param sheaf
     *            The page manager.
     * @return The dereferenced block page or null if the block has moved.
     */
    public BlockPage getBlockPage(Sheaf sheaf)
    {
        AddressPage addresses = sheaf.getPage(address, AddressPage.class, new AddressPage());
        if (addresses.dereference(address) == position)
        {
            if (rawPage.getPage() instanceof BlockPage)
            {
                return (BlockPage) rawPage.getPage();
            }
            return sheaf.getPage(rawPage.getPosition(), BlockPage.class, new BlockPage());
        }
        return null;
    }

    /**
     * Get the monitor for the underlying byte buffer for the raw page.
     * 
     * @return The raw page monitor.
     */
    public Object getMonitor()
    {
        return rawPage;
    }
}
