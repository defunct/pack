package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

class Commit extends Operation
{
    public Commit()
    {
    }

    private void commit(Sheaf sheaf, JournalHeader journalHeader, DirtyPageSet dirtyPages)
    {
        journalHeader.getByteBuffer().clear();
        journalHeader.getByteBuffer().putLong(0, 0L);

        dirtyPages.flush();
        journalHeader.write(sheaf.getFileChannel());
        try
        {
            sheaf.getFileChannel().force(true);
        }
        catch (IOException e)
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
        return Pack.FLAG_SIZE;
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.COMMIT);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
    }
}
