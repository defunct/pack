package com.goodworkalan.pack;

import java.nio.ByteBuffer;

/**
 * Terminate journal playback by returning true from the {@link #terminate()}
 * method. This journal operation is written at the end of a journal to
 * terminate the journal playback loop.
 * <p>
 * Although it seems like this should be merged with {@link Commit}, the
 * separate instruction means that playback can terminate after a
 * {@link Checkpoint} if that ever becomes necessary. The caller might want to
 * perform concurrency housekeeping after a checkpoint, to send on there way any
 * threads waiting on resources that have been released due to the checkpoint.
 * 
 * @author Alan Gutierrez
 */
final class Terminate
extends Operation
{
    /**
     * Construct an empty instance that can be populated with the
     * {@link #read(ByteBuffer) read} method.
     */
    public Terminate()
    {
    }

    /**
     * Return true to terminate the journal playback.
     * 
     * @return True to terminate the journal playback.
     */
    @Override
    public boolean terminate()
    {
        return true;
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
        return Pack.SHORT_SIZE;
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
        bytes.putShort(TERMINATE);
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
    }
}