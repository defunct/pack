package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Commit a journal by zeroing the journal header. The journal header references
 * the file position of the first journal operation. By setting that value to
 * zero, we indicate that the journal is complete.
 * <p>
 * The journal header will be returned to the journal header pool by the journal
 * player.
 * 
 * @author Alan Gutierrez
 */
class Commit extends Operation
{
    /**
     * Construct a commit instance. The commit operation has no properties.
     */
    public Commit()
    {
    }

    /**
     * Flush all dirty pages and write a zero into the journal header before
     * forcing all changes to the underlying file channel.
     * 
     * @param sheaf
     *            The underlying <code>Sheaf</code> page manager.
     * @param journalHeader
     *            The journal header record.
     * @param dirtyPages
     *            The dirty page set.
     */
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

    /**
     * Flush all dirty pages and write a zero into the journal header before
     * forcing all changes to the underlying file channel.
     * 
     * @param player
     *            The journal player.
     */
    @Override
    public void commit(Player player)
    {
        commit(player.getBouquet().getSheaf(), player.getJournalHeader(), player.getDirtyPages());
    }
    
    /**
     * Return the length of the operation in the journal including the type
     * flag.
     * 
     * @return The length of this operation in the journal.
     */
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE;
    }

    /**
     * Write the operation type flag and the operation data to the given byte
     * buffer.
     * 
     * @param bytes
     *            The byte buffer.
     */
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.COMMIT);
    }
    
    /**
     * Return the length of the operation in the journal including the type
     * flag.
     * 
     * @return The length of this operation in the journal.
     */
    @Override
    public void read(ByteBuffer bytes)
    {
    }
}
