package com.goodworkalan.pack;

import java.nio.ByteBuffer;

/**
 * A journal operation that will jump the journal playback to a specific journal
 * operation at a specific page position. This journal operation is used to
 * continue journals across multiple journal pages.
 * 
 * @author Alan Gutierrez
 */
final class NextOperation
extends Operation
{
    /** The file position of the next operation. */
    private long position;

    /**
     * Construct an empty instance that can be populated with the
     * {@link #read(ByteBuffer) read} method.
     */
    public NextOperation()
    {
    }

    /**
     * Create a next operation operation that will return the journal page
     * of the given operation position offset to the operation position offset.
     * 
     * @param position The operation position.
     */
    public NextOperation(long position)
    {
        this.position = position;
    }
    
    /**
     * Return the journal page indicated by the position of the next operation
     * of this next operation operation with the journal page offset set to
     * reference the next operation.
     * 
     * @param player
     *            The journal player.
     * @param journalPage
     *            The current journal page.
     * @return The journal page of the next operation.
     */
    @Override
    public JournalPage getJournalPage(Player player, JournalPage journalPage)
    {
        journalPage = player.getBouquet().getSheaf().getPage(player.getBouquet().getAddressBoundary().adjust(player.getBouquet().getSheaf(), position), JournalPage.class, new JournalPage());
        journalPage.seek(player.getBouquet().getAddressBoundary().adjust(player.getBouquet().getSheaf(), position));
        return journalPage;
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
        bytes.putShort(NEXT_OPERATION);
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