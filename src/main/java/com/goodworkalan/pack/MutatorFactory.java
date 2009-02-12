package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;

class MutatorFactory
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
        DirtyPageSet dirtyPages = new DirtyPageSet(16);
        Journal journal = new Journal(bouquet.getSheaf(), bouquet.getInterimPagePool(), dirtyPages);
        return new Mutator(bouquet, journal, dirtyPages);
    }
}
