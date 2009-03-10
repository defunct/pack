package com.goodworkalan.pack;

import java.nio.ByteBuffer;

/**
 * Base class for all journal operations.
 * 
 * @author Alan Gutierrez
 */
abstract class Operation
{
    /** Flag used to indicate a {@link NextOperation} operation on disk. */
    final static short NEXT_OPERATION = 1;

    /** Flag used to indicate a {@link MovePage} operation on disk. */
    final static short MOVE_PAGE = 2;

    /** Flag used to indicate a {@link CreateAddressPage} operation on disk. */
    final static short CREATE_ADDRESS_PAGE = 3;

    /** Flag used to indicate a {@link Write} operation on disk. */
    final static short WRITE = 4;

    /** Flag used to indicate a {@link Free} operation on disk. */
    final static short FREE = 5;

    /** Flag used to indicate a {@link Temporary} operation on disk. */
    final static short TEMPORARY = 6;

    /** Flag used to indicate a {@link Move} operation on disk. */
    final static short MOVE = 7;

    /** Flag used to indicate a {@link Checkpoint} operation on disk. */
    final static short CHECKPOINT = 8;

    /** Flag used to indicate a {@link Commit} operation on disk. */
    final static short COMMIT = 9;

    /** Flag used to indicate a {@link Terminate} operation on disk. */
    final static short TERMINATE = 10;

    /**
     * Create a blank operation of a type corresponding to the given type flag.
     * The blank operation is then loaded from the journal page at the current
     * offset into the journal page.
     * 
     * @param type
     *            The type of operation.
     * @return An operation corresponding to the given type flag.
     */
    public static Operation newOperation(short type)
    {
        switch (type)
        {
        case NEXT_OPERATION:
            return new NextOperation();
        case MOVE_PAGE:
            return new MovePage();
        case CREATE_ADDRESS_PAGE:
            return new CreateAddressPage();
        case WRITE:
            return new Write();
        case FREE:
            return new Free();
        case TEMPORARY:
            return new Temporary();
        case MOVE:
            return new Move();
        case CHECKPOINT:
            return new Checkpoint();
        case COMMIT:
            return new Commit();
        case TERMINATE:
            return new Terminate();
        }
        throw new IllegalStateException("Invalid type: " + type);
    }

    /**
     * Execute the operation. Subclasses will override this method to provide
     * logic for specific journal operations.
     * <p>
     * This default implementation does nothing.
     * 
     * @param player
     *            The journal player.
     */
    public void execute(Player player)
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