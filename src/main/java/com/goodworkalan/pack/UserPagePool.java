package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.goodworkalan.sheaf.DirtyPageSet;

class UserPagePool
{
    private final Set<Long> freedBlockPages;
    
    private final Set<Long> allocatedBlockPages;
    
    private final int alignment;
    
    /**
     * Set of empty user pages. This set is checked for empty pages to store
     * allocations during commit. To reuse an empty user page, the page is
     * removed from the set, so that no other mutator will attempt to use it to
     * write block allocations. Once the commit is complete, all user pages with
     * space remaining are added to the free page by size table, or the free
     * page set if the user page is completely empty.
     */
    private final FreeSet emptyUserPages;
    
    /**
     * A table that orders user pages with block space available by the size of
     * bytes remaining. During commit, this table is checked for user pages that
     * can accommodate block allocations. To use user page, the page is removed
     * from the table, so that no other mutator will attempt to use it to write
     * block allocations. Once the commit is complete, all user pages with space
     * remaining are added to the free page by size table, or the free page set
     * if the user page is completely empty.
     */
    private final ByRemainingTable freePageBySize;

    public UserPagePool(int pageSize, int alignment)
    {
        this.alignment = alignment;
        this.freePageBySize = new ByRemainingTable(pageSize, alignment);
        this.emptyUserPages = new FreeSet();
        this.freedBlockPages = new HashSet<Long>();
        this.allocatedBlockPages = new HashSet<Long>();

    }

    /**
     * Return the set of completely empty user pages available for block
     * allocation. The set returned is a class that not only contains the set of
     * pages available, but will also prevent a page from being returned to the
     * set of free pages, if that page is midst of relocation.
     * <p>
     * A user page is used by one mutator commit at a time. Removing the page
     * from this table prevents it from being used by another commit.
     * <p>
     * Removing a page from this set, prevents it from being used by an
     * destination for allocations or writes. Removing a page from the available
     * pages sets is the first step in relocating a page.
     * 
     * @return The set of free user pages.
     */
    public FreeSet getEmptyUserPages()
    {
        return emptyUserPages;
    }

    /**
     * Return a best fit lookup table of user pages with space remaining for
     * block allocation. This lookup table is used to find destinations for
     * newly allocated user blocks during commit.
     * <p>
     * A user page is used by one mutator commit at a time. Removing the page
     * from this table prevents it from being used by another commit.
     * <p>
     * Removing a page from this set, prevents it from being used by an
     * allocation. Removing a page from the available pages sets is the first
     * step in relocating a page.
     * <p>
     * Note that here is no analgous list of free interim pages by size, since
     * interim block pages are not shared between mutators and they are
     * completely reclaimed at the end of a mutation.
     * 
     * @return The best fit lookup table of user pages.
     */
    public ByRemainingTable getFreePageBySize()
    {
        return freePageBySize;
    }

    /**
     * Return a user page to the free page accounting, if the page has any 
     * space remaining for blocks. If the block page is empty, it is added
     * to the set of empty user pages. If it has block space remaining that
     * is greater than the alignment, then it is added to by size lookup table.
     * 
     * @param user The user block page.
     */
    public void returnUserPage(BlockPage user)
    {
        if (user.getBlockCount() == 0)
        {
            emptyUserPages.free(user.getRawPage().getPosition());
        }
        else if (user.getRemaining() > alignment)
        {
            getFreePageBySize().add(user);
        }
    }
    
    public void add(Set<Long> freedBlockPages, Set<Long> allocatedBlockPages)
    {
        synchronized (freedBlockPages)
        {
            this.freedBlockPages.addAll(freedBlockPages);
        }
        synchronized (allocatedBlockPages)
        {
            this.allocatedBlockPages.addAll(allocatedBlockPages);
        }
    }
    
    private Set<Long> getFreedBlockPages()
    {
        Set<Long> copy = new HashSet<Long>(freedBlockPages);
        freedBlockPages.clear();
        return copy;
    }

    private Set<Long> getAllocatedBlockPages()
    {
        Set<Long> copy = new HashSet<Long>(allocatedBlockPages);
        allocatedBlockPages.clear();
        return copy;
    }
    
    public synchronized void vacuum(Bouquet bouquet)
    {
        Set<Long> allocatedBlockPages = getAllocatedBlockPages();
        Set<Long> freedBlockPages = getFreedBlockPages();
        allocatedBlockPages.removeAll(freedBlockPages);

        Iterator<Long> continuous = freedBlockPages.iterator();
        while (continuous.hasNext())
        {
            long position = continuous.next();
            BlockPage blocks = bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
            synchronized (blocks.getRawPage())
            {
                if (!blocks.isContinuous())
                {
                    continuous.remove();
                }
            }
        }
        
        DirtyPageSet dirtyPages = new DirtyPageSet(16);
        Journal journal = new Journal(bouquet.getSheaf(), bouquet.getInterimPagePool(), dirtyPages);

        for (long position : freedBlockPages)
        {
            freePageBySize.remove(position);
        }
        
        int pageSize = bouquet.getSheaf().getPageSize();
        Map<Long, Long> moves = new HashMap<Long, Long>();
        Iterator<Long> discontinuous = freedBlockPages.iterator();
        while (discontinuous.hasNext())
        {
            long position = discontinuous.next();
            BlockPage blocks = bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
            long bestFit = freePageBySize.bestFit(pageSize - blocks.getRemaining());
            if (bestFit != 0)
            {
                moves.put(position, bestFit);
                discontinuous.remove();
            }
        }
        
        for (long position : freedBlockPages)
        {
            BlockPage destnation = bouquet.getInterimPagePool().newInterimPage(bouquet.getSheaf(), BlockPage.class, new BlockPage(), dirtyPages);
            moves.put(position, destnation.getRawPage().getPosition());
        }
        
        Iterator<Long> allocated = allocatedBlockPages.iterator();
        while (allocated.hasNext())
        {
            long position = allocated.next();
            BlockPage blocks = bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
            long bestFit = freePageBySize.bestFit(pageSize - blocks.getRemaining());
            if (bestFit != 0)
            {
                moves.put(position, bestFit);
                allocated.remove();
            }
        }
        
        for (Map.Entry<Long, Long> move : moves.entrySet())
        {
            BlockPage destination = bouquet.getUserBoundary().load(bouquet.getSheaf(), move.getValue(), BlockPage.class, new BlockPage());
            journal.write(new Move(move.getKey(), move.getValue(), destination.getLastAddress()));
        }

        journal.write(new Terminate());

        Player player = new Player(bouquet, journal, dirtyPages);
        player.commit();

        for (long position : allocatedBlockPages)
        {
            BlockPage blocks = bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
            freePageBySize.add(blocks);
        }
        
        for (long position : moves.values())
        {
            BlockPage blocks = bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
            freePageBySize.add(blocks);
        }
        
        for (long position : moves.keySet())
        {
            bouquet.getSheaf().free(position);
            bouquet.getInterimPagePool().getFreeInterimPages().free(position);
        }
    }
}