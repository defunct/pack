package com.goodworkalan.pack;

import java.nio.ByteBuffer;

/**
 * Base class for all journal operations.
 * 
 * @author Alan Gutierrez
 */
abstract class Operation
{
    /**
     * Execute the operation. Subclasses will override this method to provide
     * logic for specific journal operations.
     * <p>
     * This default implementation does nothing.
     * 
     * @param player
     *            The journal player.
     */
    public void commit(Player player)
    {
    }

    /**
     * Get the journal page to use to read the next journal operation.
     * <p>
     * This default implementation simply returns the given journal page. 
     * 
     * @param player
     *            The journal player.
     * @param journalPage
     *            The current journal page.
     * @return The journal page to use to read the next journal operation.
     */
    public JournalPage getJournalPage(Player player, JournalPage journalPage)
    {
        return journalPage;
    }

    /**
     * If true, the journal player should terminate playback.
     * 
     * @return True to terminate journal playback.
     */
    public boolean terminate()
    {
        return false;
    }

    /**
     * Return the length of the operation in the journal including the type
     * flag.
     * 
     * @return The length of this operation in the journal.
     */
    public abstract int length();

    /**
     * Write the operation type flag and the operation data to the given byte
     * buffer.
     * 
     * @param bytes
     *            The byte buffer.
     */
    public abstract void write(ByteBuffer bytes);

    /**
     * Read the operation data but not the preceding operation type flag from
     * the byte buffer.
     * 
     * @param bytes
     *            The byte buffer.
     */
    public abstract void read(ByteBuffer bytes);
}