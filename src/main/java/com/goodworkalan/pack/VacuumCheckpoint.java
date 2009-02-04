package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;


final class VacuumCheckpoint
extends Operation
{
    private long newJournalPosition;
    
    public VacuumCheckpoint()
    {
    }
    
    public VacuumCheckpoint(long position)
    {
        this.newJournalPosition = position;
    }

    @Override
    public void commit(Player player)
    {
        for (Vacuum addVacuum: player.getVacuumSet())
        {
            addVacuum.vacuum(player);
        }
        ByteBuffer bytes = player.getJournalHeader().getByteBuffer();
        bytes.clear();
        long oldJournalPosition = bytes.getLong();
        if (oldJournalPosition != newJournalPosition)
        {
            bytes.clear();
            bytes.putLong(newJournalPosition);
            player.getDirtyPages().flush();
            player.getJournalHeader().write(player.getBouquet().getSheaf().getFileChannel());
            try
            {
                player.getBouquet().getSheaf().getFileChannel().force(true);
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_FORCE, e);
            }
        }
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.VACUUM);
        bytes.putLong(newJournalPosition);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
        this.newJournalPosition = bytes.getLong();
    }
}