package com.goodworkalan.pack;

import java.util.TreeSet;

final class SetTracker
extends TreeSet<Long>
implements MoveTracker
{
    private static final long serialVersionUID = 1L;

    public boolean involves(long position)
    {
        return contains(position) || contains(-position);
    }
    
    public boolean record(Move move, boolean moved)
    {
        if (contains(move.getFrom()))
        {
            remove(move.getFrom());
            add(move.getTo());
            moved = true;
        }
        if (contains(-move.getFrom()))
        {
            remove(-move.getFrom());
            add(move.getFrom());
        }
        return moved;
    }
}