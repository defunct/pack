package com.goodworkalan.pack;

final class JournalMoveTracker
implements MoveTracker
{
    private final Journal journal;
    
    public JournalMoveTracker(Journal journal)
    {
        this.journal = journal;
    }

    public boolean involves(long position)
    {
        return false;
    }
    
    public boolean record(Move move, boolean moved)
    {
        // TODO Does involves take care of this? Yes. It will. It should.
        if (moved)
        {
            journal.write(new ShiftMove());
        }

        return moved;
    }
    
    public void clear()
    {
        journal.reset();
    }
}