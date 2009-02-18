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
    /** The strategy for optimizing the size of the file on disk. */
    private Vacuum vacuum;
    
    /** A set of pages with freed blocks that may require vacuum. */
    private final Set<Long> freedBlockPages;
    
    /**
     * A set of newly allocated user block pages that could be combined with
     * existing block pages.
     */
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

    /**
     * Create a user page pool for pages of the given page size and blocks
     * aligned to the given alignment.
     * 
     * @param pageSize
     *            The page size.
     * @param alignment
     *            The block alignment.
     */
    public UserPagePool(int pageSize, int alignment)
    {
        this.vacuum = new NullVacuum();
        this.byRemaining = new ByRemainingTable(pageSize, alignment);
        this.freedBlockPages = new HashSet<Long>();
        this.allocatedBlockPages = new HashSet<Long>();
    }

    /**
     * Set the strategy for optimizing the size of the file on disk.
     * 
     * @param vacuum
     *            The strategy for optimizing the size of the file on disk.
     */
    public void setVacuum(Vacuum vacuum)
    {
        this.vacuum = vacuum;
    }

    /**
     * Add the set of block pages with freed blocks and the set of newly
     * allocated block pages to the pools sets of pages that require vacuum.
     * <p>
     * This method is called by the <code>Mutator</code>
     * {@link Mutator#commit() commit} method
     * 
     * @param freedBlockPages
     *            A set of pages with freed blocks.
     * @param allocatedBlockPages
     */
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

    /**
     * Return a copy of the block pages with freed block and clear the set of
     * block pages with free blocks maintained by the pool.
     * 
     * @return A copy of the set of block pages with freed blocks.
     */
    private Set<Long> getFreedBlockPages()
    {
        synchronized (freedBlockPages)
        {
            Set<Long> copy = new HashSet<Long>(freedBlockPages);
            freedBlockPages.clear();
            return copy;
        }
    }

    /**
     * Return a copy of the newly allocated block pages and clear the set of
     * allocated block pages maintained by the pool.
     * 
     * @return A copy of the newly allocated block pages.
     */
    private Set<Long> getAllocatedBlockPages()
    {
        synchronized (allocatedBlockPages)
        {
            Set<Long> copy = new HashSet<Long>(allocatedBlockPages);
            allocatedBlockPages.clear();
            return copy;
        }
    }
    
    /**
     * Return the number of pages with space remaining.
     * 
     * @return The number of pages with space remaining.
     */
    public int getSize()
    {
        return byRemaining.getSize();
    }
    
    /**
     * Return an iterator over the pages that have space available for the
     * allocation of blocks.
     * 
     * @return An iterator over the page that have space available.
     */
    public Iterator<Long> iterator()
    {
        return byRemaining.iterator();
    }
    
    // TODO File set, each page divided into blocks with a larger and larger
    // number of entries.
    // TODO Bit set can be kept on pages, just a very plain page, read
    // and write to raw page.
    public void vacuum(Bouquet bouquet)
    {
        Set<Long> allocatedBlockPages = getAllocatedBlockPages();
        Set<Long> freedBlockPages = getFreedBlockPages();
        Set<Long> emptyBlockPages = new HashSet<Long>();
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
                if (blocks.getBlockCount() == 0)
                {
                    emptyBlockPages.add(position);
                }
            }
        }
        
        Map<Long, Long> moves = new HashMap<Long, Long>();
        
        vacuum.vacuum(new MoveRecorder(bouquet, moves), byRemaining, allocatedBlockPages, freedBlockPages);

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
        
        for (long position : emptyBlockPages)
        {
            bouquet.getSheaf().free(position);
            bouquet.getInterimPagePool().free(position);
        }
     }
}