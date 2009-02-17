package com.goodworkalan.pack;

import java.nio.ByteBuffer;

// FIXME Comment.
final class Terminate
extends Operation
{
    public Terminate()
    {
    }

    @Override
    public boolean terminate()
    {
        return true;
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE;
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.TERMINATE);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
    }
}