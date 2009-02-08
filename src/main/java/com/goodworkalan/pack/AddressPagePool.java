package com.goodworkalan.pack;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;

import com.goodworkalan.sheaf.Sheaf;

class AddressPagePool implements Iterable<Long>
{

    /** A set of address pages with available free addresses. */
    private final SortedSet<Long> addressPages;
    /**
     * A set of address pages currently checked out by a mutator to allocate a
     * single address, that have more than one free address available.
     */
    private final Set<Long> returningAddressPages;

    /**
     * Create an address page pool.
     * 
     * @param addressPages
     *            A set of address pages with available free addresses.
     */
    public AddressPagePool(SortedSet<Long> addressPages)
    {
        this.addressPages = addressPages;
        this.returningAddressPages = new HashSet<Long>();
    }

    /**
     * Return the number of address page positions currently in this pool.
     * <p>
     * This method is not synchronized because it is currently only called when
     * the compact lock is held exclusively.
     * 
     * @return The size of the pool.
     */
    public int size()
    {
        if (returningAddressPages.size() != 0)
        {
            throw new IllegalStateException();
        }
        return addressPages.size();
    }

    /**
     * Return an iterator over the address page positions in the pool.
     * <p>
     * This method is not synchronized because it is currently only called
     * when the compact lock is held exclusively.
     * 
     * @return The an iterator over the address page positions.
     */
    public Iterator<Long> iterator()
    {
        if (returningAddressPages.size() != 0)
        {
            throw new IllegalStateException();
        }
        return addressPages.iterator();
    }

    /**
     * Try to get an address page from the set of available address pages or
     * create address pages by moving user pages if there are none available or
     * outstanding.
     * <p>
     * The caller can allocate one and only address from address page returned.
     * It must then return the address page to the pager.
     * 
     * @param lastSelected
     *            The last selected address page, which we will try to return if
     *            available.
     * 
     * @return An address page with at least one free position.
     */
    private AddressPage getOrCreateAddressPage(MutatorFactory mutators, Sheaf pager, long lastSelected)
    {
        // Lock on the set of address pages, which protects the set of
        // address pages and the set of returning address pages.
        
        synchronized (addressPages)
        {
            long position = 0L;         // The position of the new address page.
    
            // If there are no address pages in the pager that have one
            // or more free addresses and there are no address pages
            // outstanding that have two or more free addresses, then we
            // need to allocate more address pages and try again.
            
            if (addressPages.size() == 0 && returningAddressPages.size() == 0)
            {
                // Create a mutator to move the user page immediately
                // following the address page region  to a new user
                // page.
                Mutator mutator = mutators.mutate();
    
                // See the source of Mutator.newAddressPage.
                addressPages.addAll(mutator.newAddressPages(1));
                
                // There are more pages now, return null indicating we need to
                // try again.
                return null;
            }
            else
            {
                // If we can return the last selected address page,
                // let's. It will help with locality of reference.
    
                if (addressPages.contains(lastSelected))
                {
                    position = lastSelected;
                }
                else if (addressPages.size() != 0)
                {
                    position = addressPages.first();
                }
                else
                {
                    // We are here because our set of returning address
                    // pages is not empty, meaning that a Mutator will
                    // be returning an address page that has some space
                    // left. We need to wait for it.
                    
                    try
                    {
                        addressPages.wait();
                    }
                    catch (InterruptedException e)
                    {
                    }
    
                    // When it arrives, we'll return null indicating we
                    // need to try again.
    
                    return null;
                }
    
                // Remove the address page from the set of address
                // pages available for allocation.
    
                addressPages.remove(position);
            }
    
            // Get the address page.
    
            AddressPage addressPage = pager.getPage(position, AddressPage.class, new AddressPage());
    
            // If the address page has two or more addresses available,
            // then we add it to the set of returning address pages, the
            // address pages that have space, but are currently in use,
            // so we should wait for them.
    
            if (addressPage.getFreeCount() > 1)
            {
                returningAddressPages.add(position);
            }
    
            // Return the address page.
    
            return addressPage;
        }
    }

    /**
     * Return an address page to the set of address pages with one or more free
     * addresses available for allocation.
     * <p>
     * The set of free address pages is used as the mutex for threads requesting
     * an address page. If the address page is in the set of returning address
     * pages, the address page is added to the set of free address pages and all
     * threads waiting on the set of address page mutex are notified.
     * 
     * @param addressPage
     *            The address page with one or more free addresses available for
     *            allocation.
     */
    public void returnAddressPage(AddressPage addressPage)
    {
        long position = addressPage.getRawPage().getPosition();
        synchronized (addressPages)
        {
            if (returningAddressPages.remove(position))
            {
                addressPages.add(position);
                addressPages.notifyAll();
            }
        }
    }

    /**
     * Return an address page with one or more free addresses available for
     * allocation. If the given last selected address page has one or more free
     * addresses available for allocation and it is not in use with another
     * mutator, then it is returned, hopefully helping locality of reference.
     * <p>
     * This method will block if all of the available address pages are in use
     * by other mutators, until one of the other address pages returns an
     * address page. If there are fewer address pages than the minimum number of
     * available address pages for allocation outstanding, then a new address
     * page is created by expanding the address page region.
     * <p>
     * TODO Specify a minimum address page pool.
     * 
     * @param lastSelected
     *            The address of the last selected address page.
     * @return An address page with one or more free addresses available for
     *         allocation.
     */
    public AddressPage getAddressPage(MutatorFactory mutatorFactory, Sheaf sheaf, long lastSelected)
    {
        for (;;)
        {
            AddressPage addressPage = getOrCreateAddressPage(mutatorFactory, sheaf, lastSelected);
            if (addressPage != null)
            {
                return addressPage;
            }
        }
    }
}
