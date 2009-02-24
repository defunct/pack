package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;

public class ByRemainingPositionIO implements PositionIO
{
    private final ByRemainingPage byRemainingTable;
    
    private final int alignmentIndex;
    
    public ByRemainingPositionIO(ByRemainingPage byRemainingTable, int alignmentIndex)
    {
        this.byRemainingTable = byRemainingTable;
        this.alignmentIndex = alignmentIndex;
    }
    
    public void write(long position, DirtyPageSet dirtyPages)
    {
        byRemainingTable.setSlotPosition(alignmentIndex, position, dirtyPages);
    }
    public long read()
    {
        return byRemainingTable.getSlotPosition(alignmentIndex);
    }
    
    public void writeAlloc(int slotIndex, long position, DirtyPageSet dirtyPages)
    {
        byRemainingTable.setAllocSlotPosition(slotIndex, position, dirtyPages);
    }
    
    public long readAlloc(int slotIndex)
    {
        return byRemainingTable.getAllocSlotPosition(slotIndex);
    }
}
