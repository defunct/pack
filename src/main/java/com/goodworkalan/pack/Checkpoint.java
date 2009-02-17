package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

// FIXME Comment.
class Checkpoint extends Operation
{
    private long position;
    
    public Checkpoint()
    {
    }
    
    public Checkpoint(long position)
    {
        this.position = position + length();
    }

    private void commit(Sheaf sheaf, JournalHeader journalHeader, DirtyPageSet dirtyPages)
    {
        dirtyPages.flush();
        
        journalHeader.getByteBuffer().clear();
        journalHeader.getByteBuffer().putLong(0, position);

        journalHeader.write(sheaf.getFileChannel());
        
        try
        {
            sheaf.getFileChannel().force(false);
        }
        catch(IOException e)
        {
            throw new PackException(PackException.ERROR_IO_FORCE, e);
        }
    }

    @Override
    public void commit(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getJournalHeader(), player.getDirtyPages());
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE;
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.CHECKPOINT);
        bytes.putLong(position);
    }

    @Override
    public void read(ByteBuffer bytes)
    {
        position = bytes.getLong();
    }
}
