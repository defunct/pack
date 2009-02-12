package com.goodworkalan.pack;

import java.util.HashMap;
import java.util.Map;

public class MoveMap
{
    private final Map<Long, Long> moves;
    
    public MoveMap()
    {
        moves = new HashMap<Long, Long>();
    }
    
    public synchronized void putAll(Map<Long, Long> moves)
    {
        moves.putAll(moves);
    }
    
    public synchronized long get(long from)
    {
        if (!moves.containsKey(from))
        {
            try
            {
                wait();
            }
            catch (InterruptedException e)
            {
            }
        }
        return moves.get(from);
    }
}
