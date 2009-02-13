package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.goodworkalan.sheaf.DirtyPageSet;

class AddressPagePool implements Iterable<Long>
{
    private final int addressPagePoolSize;
    
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
    public AddressPagePool(int addressPagePoolSize, SortedSet<Long> addressPages)
    {
        this.addressPagePoolSize = addressPagePoolSize;
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
     * Create address pages, extending the address page region by moving the
     * user pages immediately follow after locking the pager to prevent
     * compaction and close.
     * <p>
     * Remember that this method is already guarded in <code>Pager</code> by
     * synchronizing on the set of free addresses.
     * 
     * @param newAddressPageCount
     *            The number of address pages to allocate.
     */
    private SortedSet<Long> tryNewAddressPage(Bouquet bouquet, int newAddressPageCount)
    {
        // The set of newly created address pages.
        SortedSet<Long> newAddressPages = new TreeSet<Long>();
        Set<Long> pagesToMove = new HashSet<Long>();
        
        // Now we know we have enough user pages to accommodate our creation of
        // address pages. That is we have enough user pages, full stop. We have
        // not looked at whether they are free or in use.
        
        // Some of those user block pages may not yet exist. We are going to
        // have to wait until they exist before we do anything with with the
        // block pages.

        for (int i = 0; i < newAddressPageCount; i++)
        {
            // The new address page is the user page at the user page boundary.
            long position = bouquet.getUserBoundary().getPosition();
            
            // Record the new address page.
            newAddressPages.add(position);
            
            
            // If new address page was not just created by relocating an interim
            // page, then we do not need to reserve it.
        
            // If the position is not in the free page by size, then we'll
            // attempt to reserve it from the list of free user pages.

            if (!bouquet.getInterimPagePool().remove(position))
            {
                pagesToMove.add(position);
            }

            // Move the boundary for user pages.
            bouquet.getUserBoundary().increment();
        }

        // To move a data page to make space for an address page, we simply copy
        // over the block pages that need to move, verbatim into an interim
        // block page and create a commit. The commit method will see these
        // interim block pages will as allocations, it will allocate the
        // necessary user pages and move them into a new place in the user
        // region.

        // The way that journals are written, vacuums and copies are written
        // before the operations gathered during mutation are written, so we
        // write out our address page initializations now and they will occur
        // after the blocks are copied.

        
        // If the new address page is in the set of free block pages or if it is
        // a block page we've just created the page does not have to be moved.
        
        DirtyPageSet dirtyPages = new DirtyPageSet(16);
        Journal journal = new Journal(bouquet.getSheaf(), bouquet.getInterimPagePool(), dirtyPages);
        Map<Long, Long> moves = new HashMap<Long, Long>();
        for (long from : pagesToMove)
        {
            // Allocate mirrors for the user pages and place them in
            // the alloc page by size table and the allocation page set
            // so the commit method will assign a destination user page.
            long to = bouquet.getInterimPagePool().newBlankInterimPage(bouquet.getSheaf());
            journal.write(new MovePage(from, to));
            moves.put(from, to);
        }
        
        journal.write(new Checkpoint(journal.getJournalPosition()));
        
        for (long position : newAddressPages)
        {
            long to = moves.containsKey(position) ? moves.get(position) : 0L;
            journal.write(new CreateAddressPage(position, to));
        }
        
        journal.write(new Commit());
        journal.write(new Terminate());
        
        new Player(bouquet, journal, dirtyPages).commit();
        
        return newAddressPages;
    }

    /**
     * Create address pages, extending the address page region by moving the
     * user pages immediately follow after locking the pager to prevent
     * compaction and close.
     * 
     * @param count
     *            The number of address pages to create.
     */
    SortedSet<Long> newAddressPages(Bouquet bouquet, int count)
    {
        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.
        bouquet.getPageMoveLock().writeLock().lock();
        try
        {
            return tryNewAddressPage(bouquet, count); 
        }
        finally
        {
            bouquet.getPageMoveLock().readLock().unlock();
        }
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
    private AddressPage getOrCreateAddressPage(Bouquet bouquet, long lastSelected)
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
            
            if (addressPages.size() + returningAddressPages.size() < addressPagePoolSize)
            {
                // Create a mutator to move the user page immediately
                // following the address page region  to a new user
                // page.
                // See the source of Mutator.newAddressPage.
                newAddressPages(bouquet, addressPagePoolSize - (addressPages.size() + returningAddressPages.size()));
                
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
            AddressPage addressPage = bouquet.getSheaf().getPage(position, AddressPage.class, new AddressPage());
    
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
     * 
     * @param lastSelected
     *            The address of the last selected address page.
     * @return An address page with one or more free addresses available for
     *         allocation.
     */
    public AddressPage getAddressPage(Bouquet bouquet, long lastSelected)
    {
        for (;;)
        {
            AddressPage addressPage = getOrCreateAddressPage(bouquet, lastSelected);
            if (addressPage != null)
            {
                return addressPage;
            }
        }
    }
}
