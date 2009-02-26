package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;

// TODO Document.
public class ByRemainingPositionIO implements LookupBlockPositionIO
{
    // TODO Document.
    private final ByRemainingPage byRemainingPage;
    
    // TODO Document.
    private final int alignmentIndex;
    
    // TODO Document.
    public ByRemainingPositionIO(ByRemainingPage byRemainingPage, int alignmentIndex)
    {
        this.byRemainingPage = byRemainingPage;
        this.alignmentIndex = alignmentIndex;
    }
    
    // TODO Document.
    public void write(long position, DirtyPageSet dirtyPages)
    {
        byRemainingPage.setSlotPosition(alignmentIndex, position, dirtyPages);
    }

    // TODO Document.
    public long read()
    {
        return byRemainingPage.getSlotPosition(alignmentIndex);
    }
}
