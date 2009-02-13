package com.goodworkalan.pack;

import java.util.Map;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

public class Mover
{
    private final Map<Long, Long> moves;

    private final Bouquet bouquet;
    
    private DirtyPageSet dirtyPages;

    public Mover(Bouquet bouquet, Map<Long, Long> moves)
    {
        this.bouquet = bouquet;
        this.moves = moves;
    }
    
    public Sheaf getSheaf()
    {
        return bouquet.getSheaf();
    }
    
    public BlockPage getBlockPage(long position)
    {
        return bouquet.getUserBoundary().load(bouquet.getSheaf(), position, BlockPage.class, new BlockPage());
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
