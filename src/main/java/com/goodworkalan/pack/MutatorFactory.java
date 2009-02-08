package com.goodworkalan.pack;

import java.util.List;

import com.goodworkalan.sheaf.DirtyPageSet;

public class MutatorFactory
{
    private final Bouquet bouquet;
    
    public MutatorFactory(Bouquet bouquet)
    {
        this.bouquet = bouquet;
    }

    /**
     * Create a <code>Mutator</code> to inspect and alter the contents of this
     * pack.
     * 
     * @return A new mutator.
     */
    public Mutator mutate()
    {
        final PageMoveTracker pageRecorder = new PageMoveTracker();
        final MoveLatchIterator moveLatchIterator = bouquet.getMoveLatchList().newIterator(pageRecorder);
        return moveLatchIterator.mutate(new Guarded<Mutator>()
        {
            public Mutator run(List<MoveLatch> listOfMoveLatches)
            {
                MoveNodeRecorder moveNodeRecorder = new MoveNodeRecorder();
                DirtyPageSet dirtyPages = new DirtyPageSet(16);
                Journal journal = new Journal(bouquet.getSheaf(), bouquet.getInterimPagePool(), moveNodeRecorder, pageRecorder, dirtyPages);
                return new Mutator(bouquet, moveLatchIterator, moveNodeRecorder, pageRecorder, journal, dirtyPages);
            }
        });
    }
}
