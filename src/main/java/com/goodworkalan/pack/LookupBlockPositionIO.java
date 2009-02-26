package com.goodworkalan.pack;

import com.goodworkalan.sheaf.DirtyPageSet;

// TODO Document.
public interface LookupBlockPositionIO
{
    // TODO Document.
    public void write(long position, DirtyPageSet dirtyPages);
    
    // TODO Document.
    public long read();
}
