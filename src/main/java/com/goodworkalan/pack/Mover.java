package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

public class Mover
{
    private final Map<Long, Long> moves;

    private final Bouquet bouquet;
    
    private DirtyPageSet dirtyPages;

    Mover(Bouquet bouquet, Map<Long, Long> moves)
    {
        this.bouquet = bouquet;
        this.moves = moves;
    }
    
    public Sheaf getSheaf()
    {
        return bouquet.getSheaf();
    }

    /**
     * Get the block page at the given position.
     * 
     * @param position
     *            The position of the block page.
     * @return The block page.
     */
    private BlockPage getBlockPage(long position)
    {
        return bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
    }

    /**
     * Get the bytes remaining in the block page at the given position.
     * 
     * @param position
     *            The position of the block page.
     * @return The bytes remaining in the block page.
     */
    public int getBytesRemaining(long position)
    {
        return getBlockPage(position).getRemaining();
    }

    /**
     * Return a map of addresses to block sizes in the block page at the given
     * position.
     * 
     * @param position
     *            The position of the block page.
     * @return A map of addresses to block sizes.
     */
    public Map<Long, Integer> getBlockSizes(long position)
    {
        Map<Long, Integer> map = new HashMap<Long, Integer>();
        BlockPage blocks = getBlockPage(position);
        synchronized (blocks.getRawPage())
        {
            for (long address : blocks.getAddresses())
            {
                map.put(address, blocks.getBlockSize(address));
            }
        }
        return map;
    }
    
    public void move(long from, long to)
    {
        moves.put(from, to);
    }
    
    public void move(long from)
    {
        BlockPage destnation = bouquet.getInterimPagePool().newInterimPage(bouquet.getSheaf(), BlockPage.class, new BlockPage(), dirtyPages, true);
        moves.put(from, destnation.getRawPage().getPosition());
    }
}
