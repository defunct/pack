package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

// TODO Comment.
class Checkpoint extends Operation
{
    /**
     * The position of the operation that will replace the current starting
     * operation in the journal header.
     */
    private long position;

    /**
     * Construct an empty instance that can be populated with the
     * {@link #read(ByteBuffer) read} method.
     */
    public Checkpoint()
    {
    }

    /**
     * Create a checkpoint operation that will replace the current starting
     * operation in the journal header with the given journal operation
     * position.
     * 
     * @param position
     *            The position of the operation that will replace the current
     *            starting operation in the journal header.
     */
    public Checkpoint(long position)
    {
        this.position = position + length();
    }

    /**
     * Update the journal header to reference the journal operation indicated by
     * the position property of this checkpoint operation. All dirty pages are
     * first flushed. After updating the header, the changes are forced to the
     * underlying file channel.
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

    /**
     * Update the journal header to reference the journal operation indicated by
     * the position property of this checkpoint operation.
     * 
     * @param player
     *            The journal player.
     */
    @Override
    public void execute(Player player)
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
        return Pack.SHORT_SIZE + Pack.LONG_SIZE;
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
        bytes.putShort(CHECKPOINT);
        bytes.putLong(position);
    }

    /**
     * Read the operation data but not the preceding operation type flag from
     * the byte buffer.
     * 
     * @param bytes
     *            The byte buffer.
     */
    @Override
    public void read(ByteBuffer bytes)
    {
        position = bytes.getLong();
    }
}
