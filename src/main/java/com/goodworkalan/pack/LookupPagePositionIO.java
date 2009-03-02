package com.goodworkalan.pack;

import java.util.List;

import com.goodworkalan.sheaf.DirtyPageSet;

// TODO Document.
public interface LookupPagePositionIO
{
    // TODO Document.
    public void write(int blockSizeIndex, long position, DirtyPageSet dirtyPages);
    
    // TODO Document.
    public long read(int blockSizeIndex);
    
    // TODO Document.
    public List<Integer> getBlockSizes();
}
