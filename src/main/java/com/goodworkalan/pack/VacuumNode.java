package com.goodworkalan.pack;

import java.nio.ByteBuffer;

public class VacuumNode
{
    public final static int SIZE = (Long.SIZE / Byte.SIZE) * 2;
    
    private long address;
    
    private long nextVacuumNode;
    
    public VacuumNode()
    {
    }
    
    public void setAddress(long address)
    {
        this.address = address;
    }

    public long getAddress()
    {
        return address;
    }
    
    public void setNextVacuumNode(long nextVacuumNode)
    {
        this.nextVacuumNode = nextVacuumNode;
    }

    public long getNextVacuumNode()
    {
        return nextVacuumNode;
    }
    
    public void write(ByteBuffer destination)
    {
        destination.putLong(address);
        destination.putLong(nextVacuumNode);
    }
    
    public void read(ByteBuffer source)
    {
        address = source.getLong();
        nextVacuumNode = source.getLong();
    }
}
