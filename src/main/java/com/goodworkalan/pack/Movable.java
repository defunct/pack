package com.goodworkalan.pack;

/**
 * A reference to a file position adjusts the file position based on the
 * page moves since the creation of the reference.
 *
 * @author Alan Gutierrez
 */
class Movable
{
    /** The root move node.  */
    private final MoveNode moveNode;
    
    /** The file position to track. */
    private final long position;
    
    /** The number of moves to skip (0 or 1). */
    private final int skip;
    
    /**
     * Create a reference to a file position that will adjust the
     * position based on the file moved appeneded to the given move
     * node, skipping the number of move nodes given by skip.
     *
     * @param moveNode The head of a list of move nodes.
     * @param position The file position to track.
     * @param skip The number of moves to skip (0 or 1). 
     */
    public Movable(MoveNode moveNode, long position, int skip)
    {
        this.moveNode = moveNode;
        this.position = position;
        this.skip = skip;
    }

    /**
     * Return the file position referenced by this movable reference adjusting
     * the file position based on page moves since the creation of the
     * reference.
     * <p>
     * The adjustment will account for offset into the page position. This is
     * necessary for next operations in journals, which may jump to any
     * operation in a journal, which may be at any location in a page.
     * 
     * @param bouquet
     *            The pager.
     */
    public long getPosition(int pageSize)
    {
        return adjust(moveNode, position, skip, pageSize);
    }

    /**
     * Return a file position based on the given file position adjusted by the
     * linked list of page moves appended to the given move node. The adjustment
     * will skip the number of move nodes given by skip.
     * <p>
     * The adjustment will account for offset into the page position. This is
     * necessary for next operations in journals, which may jump to any
     * operation in a journal, which may be at any location in a page.
     * 
     * @param moveNode
     *            The head of a list of move nodes.
     * @param position
     *            The file position to track.
     * @param skip
     *            The number of moves to skip (0 or 1).
     * @param pageSize
     *            The page size.
     * @return The file position adjusted by the recorded page moves.
     */
    private static long adjust(MoveNode moveNode, long position, int skip, int pageSize)
    {
        int offset = (int) (position % pageSize);
        position = position - offset;
        while (moveNode.getNext() != null)
        {
            moveNode = moveNode.getNext();
            Move move = moveNode.getMove();
            if (move.getFrom() == position)
            {
                if (skip == 0)
                {
                    position = move.getTo();
                }
                else
                {
                    skip--;
                }
            }
        }
        return position + offset;
    }
}

/* vim: set tw=80 : */