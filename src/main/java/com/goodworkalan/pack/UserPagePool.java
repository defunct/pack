package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.goodworkalan.sheaf.DirtyPageSet;

class UserPagePool implements Iterable<Long>
{
    private final Set<Long> freedBlockPages;
    
    private final Set<Long> allocatedBlockPages;
    
    /**
     * A table that orders user pages with block space available by the size of
     * bytes remaining. During commit, this table is checked for user pages that
     * can accommodate block allocations. To use user page, the page is removed
     * from the table, so that no other mutator will attempt to use it to write
     * block allocations. Once the commit is complete, all user pages with space
     * remaining are added to the free page by size table, or the free page set
     * if the user page is completely empty.
     */
    private final ByRemainingTable byRemaining;

    public UserPagePool(int pageSize, int alignment)
    {
        this.byRemaining = new ByRemainingTable(pageSize, alignment);
        this.freedBlockPages = new HashSet<Long>();
        this.allocatedBlockPages = new HashSet<Long>();
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
    
    public int getSize()
    {
        return byRemaining.getSize();
    }
    
    public Iterator<Long> iterator()
    {
        return byRemaining.iterator();
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
            byRemaining.remove(position);
        }
        
        int pageSize = bouquet.getSheaf().getPageSize();
        Map<Long, Long> moves = new HashMap<Long, Long>();
        Iterator<Long> discontinuous = freedBlockPages.iterator();
        while (discontinuous.hasNext())
        {
            long position = discontinuous.next();
            BlockPage blocks = bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
            long bestFit = byRemaining.bestFit(pageSize - blocks.getRemaining());
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
            long bestFit = byRemaining.bestFit(pageSize - blocks.getRemaining());
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

        journal.write(new Commit());
        journal.write(new Terminate());

        Player player = new Player(bouquet, journal, dirtyPages);
        player.commit();

        for (long position : allocatedBlockPages)
        {
            BlockPage blocks = bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
            byRemaining.add(blocks);
        }
        
        for (long position : moves.values())
        {
            BlockPage blocks = bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
            byRemaining.add(blocks);
        }
        
        for (long position : moves.keySet())
        {
            bouquet.getSheaf().free(position);
            bouquet.getInterimPagePool().free(position);
        }
    }
}