package com.goodworkalan.pack;

import java.nio.ByteBuffer;

class MovePage extends Operation
{
    private long from;
    
    private long to;

    /**
     * Default constructor builds an empty instance that can be populated
     * with the <code>read</code> method.
     */
    public MovePage()
    {
    }

    /**
     * Construct an instance that will write the relocatable page move to the
     * journal using the <code>write</code> method.
     * 
     * @param from The position to move from.
     * @param to The position to move to.
     */
    public MovePage(long from, long to)
    {
        this.from = from;
        this.to = to;
    }

    /**
     * Restore a page move to the list of moves in the given player.
     * 
     * @param player
     *            The journal player.
     */
    @Override
    public void commit(Player player)
    {
        player.getBouquet().getSheaf().move(from, to);
    }

    /**
     * Return the length of this operation in the journal including the type
     * flag.
     * 
     * @return The length of this operation in the journal.
     */
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.POSITION_SIZE * 2;
    }

    /**
     * Read the operation data but not the preceeding operation type flag from
     * the byte buffer.
     * 
     * @param bytes
     *            The byte buffer.
     */
    @Override
    public void read(ByteBuffer bytes)
    {
        from = bytes.getLong();
        to = bytes.getLong();
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
        bytes.putShort(Pack.MOVE_PAGE);
        bytes.putLong(from);
        bytes.putLong(to);
    }

}
