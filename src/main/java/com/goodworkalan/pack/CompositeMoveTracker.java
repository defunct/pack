package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.List;

class CompositeMoveTracker
implements MoveTracker
{
    private final List<MoveTracker> listOfMoveRecorders;
    
    public CompositeMoveTracker()
    {
        this.listOfMoveRecorders = new ArrayList<MoveTracker>();
    }
    
    public void add(MoveTracker recorder)
    {
        listOfMoveRecorders.add(recorder);
    }
    
    public boolean involves(long position)
    {
        for (MoveTracker recorder: listOfMoveRecorders)
        {
            if (recorder.involves(position))
            {
                return true;
            }
        }
        return false;
    }
    
    public boolean record(Move move, boolean moved)
    {
        for (MoveTracker recorder: listOfMoveRecorders)
        {
            moved = recorder.record(move, moved);
        }
        return moved;
    }
    
    public void clear()
    {
        for (MoveTracker recorder: listOfMoveRecorders)
        {
            recorder.clear();
        }
    }
}