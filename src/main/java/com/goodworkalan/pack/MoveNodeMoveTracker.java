package com.goodworkalan.pack;

final class MoveNodeMoveTracker
implements MoveTracker
{
    private MoveNode firstMoveNode;

    private MoveNode moveNode;
    
    public MoveNodeMoveTracker()
    {
        firstMoveNode = moveNode = new MoveNode();
    }

    public MoveNode getFirstMoveNode()
    {
        return firstMoveNode;
    }

    public MoveNode getMoveNode()
    {
        return moveNode;
    }
    
    public boolean involves(long position)
    {
        return false;
    }
    
    public boolean record(Move move, boolean moved)
    {
        if (moved)
        {
            moveNode = moveNode.extend(move);
        }
        return moved;
    }
    
    public void clear()
    {
        firstMoveNode = moveNode;
    }
}