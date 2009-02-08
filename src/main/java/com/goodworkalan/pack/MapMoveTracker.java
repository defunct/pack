package com.goodworkalan.pack;

import java.util.TreeMap;

final class MapMoveTracker
extends TreeMap<Long, Movable>
implements MoveTracker
{
    private static final long serialVersionUID = 1L;

    public boolean involves(long position)
    {
        return containsKey(position) || containsKey(-position);
    }
    
    public boolean record(Move move, boolean moved)
    {
        if (containsKey(move.getFrom()))
        {
            put(move.getTo(), remove(move.getFrom()));
            moved = true;
        }
        if (containsKey(-move.getFrom()))
        {
            put(move.getFrom(), remove(-move.getFrom()));
        }
        return moved;
    }
}