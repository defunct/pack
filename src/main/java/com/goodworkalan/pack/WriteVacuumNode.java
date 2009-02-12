package com.goodworkalan.pack;

import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

public class WriteVacuumNode extends Operation
{
    long vacuumNodeAddress;
    
    long journalAddress;
    
    public WriteVacuumNode()
    {
    }
    
    public WriteVacuumNode(long vacuumNodeAddress, long journalAddress)
    {
        this.vacuumNodeAddress = vacuumNodeAddress;
        this.journalAddress = journalAddress;
    }
    
    public void commit(Sheaf sheaf, UserBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        ByteBuffer bytes = ByteBuffer.allocate(VacuumNode.SIZE);

        boolean moved = true;
        do
        {
            BlockPage blocks = userBoundary.dereference(sheaf, vacuumNodeAddress);
            synchronized (blocks.getRawPage())
            {
                if (blocks.read(vacuumNodeAddress, bytes) != null)
                {
                    dirtyPages.add(blocks.getRawPage());
                    
                    bytes.flip();
                    
                    VacuumNode vacuumNode = new VacuumNode();
                    vacuumNode.read(bytes);
                    
                    bytes.flip();
                    
                    vacuumNode.setAddress(vacuumNodeAddress);
                    vacuumNode.write(bytes);
                    
                    bytes.flip();
                    
                    blocks.write(vacuumNodeAddress, bytes, dirtyPages);
                    
                    moved = false;
                }
            }
        }
        while (moved);
        
    }
    @Override
    public void commit(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getBouquet()
                .getUserBoundary(), player.getDirtyPages());
    }
    
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.WRITE_VACUUM_NODE);
        bytes.putLong(vacuumNodeAddress);
        bytes.putLong(journalAddress);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
        vacuumNodeAddress = bytes.getLong();
        journalAddress = bytes.getLong();
    }
}
