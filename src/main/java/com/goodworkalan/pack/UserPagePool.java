package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.goodworkalan.sheaf.DirtyPageSet;

// FIXME Comment.
class UserPagePool implements Iterable<Long>
{
    // FIXME Comment.
    private final Vacuum vacuum;
    
    // FIXME Comment.
    private final Set<Long> freedBlockPages;
    
    // FIXME Comment.
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
        this.vacuum = new BestFitVacuum();
        this.byRemaining = new ByRemainingTable(pageSize, alignment);
        this.freedBlockPages = new HashSet<Long>();
        this.allocatedBlockPages = new HashSet<Long>();
    }
    
    // FIXME Comment.
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
    
    // FIXME Comment.
    private Set<Long> getFreedBlockPages()
    {
        Set<Long> copy = new HashSet<Long>(freedBlockPages);
        freedBlockPages.clear();
        return copy;
    }

    // FIXME Comment.
    private Set<Long> getAllocatedBlockPages()
    {
        Set<Long> copy = new HashSet<Long>(allocatedBlockPages);
        allocatedBlockPages.clear();
        return copy;
    }
    
    // FIXME Comment.
    public int getSize()
    {
        return byRemaining.getSize();
    }
    
    // FIXME Comment.
    public Iterator<Long> iterator()
    {
        return byRemaining.iterator();
    }
    
    // TODO More magic. Create an interface.
    // TODO File set, each page divided into blocks with a larger and larger
    // number of entries.
    // TODO Bit set can be kept on pages, just a very plain page, read
    // and write to raw page.
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
        
        Map<Long, Long> moves = new HashMap<Long, Long>();
        
        vacuum.vacuum(new Mover(bouquet, moves), byRemaining, allocatedBlockPages, freedBlockPages);

        DirtyPageSet dirtyPages = new DirtyPageSet(16);
        Journal journal = new Journal(bouquet.getSheaf(), bouquet.getInterimPagePool(), dirtyPages);
        
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