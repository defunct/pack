package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;

public interface PositionIO
{
    public void write(long position, DirtyPageSet dirtyPages);
    
    public long read();
    
    public void writeAlloc(int slotIndex, long position, DirtyPageSet dirtyPages);
    
    public long readAlloc(int slotIndex);
}
