package com.goodworkalan.pack;

import java.nio.ByteBuffer;

// TODO Comment.
final class Terminate
extends Operation
{
    // TODO Comment.
    public Terminate()
    {
    }

    // TODO Comment.
    @Override
    public boolean terminate()
    {
        return true;
    }

    // TODO Comment.
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE;
    }

    // TODO Comment.
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.TERMINATE);
    }
    
    // TODO Comment.
    @Override
    public void read(ByteBuffer bytes)
    {
    }
}