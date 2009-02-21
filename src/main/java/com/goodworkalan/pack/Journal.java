package com.goodworkalan.pack;

import java.util.Set;
import java.util.zip.Adler32;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Manages a collection of journal pages linked by next operation journal
 * operations that represent a single journaled transaction.
 * 
 * @author Alan Gutierrez
 */
class Journal
{
    /** The current journal writer used to write journal operations. */
    private JournalWriter writer;

    /**
     * Create a journal using the given page manager and interim page pool to
     * allocate journal pages. The dirty page set is used to record pages that
     * have changed and need to be flushed to disk.
     * 
     * @param sheaf
     *            The page manager.
     * @param interimPagePool
     *            The interim page pool.
     * @param dirtyPages
     *            The dirty page set.
     */
    public Journal(Sheaf sheaf, InterimPagePool interimPagePool, DirtyPageSet dirtyPages)
    {
        writer = new NullJournalWriter(sheaf, interimPagePool, new Adler32(), dirtyPages);
    }

    /**
     * Get the page position and offset of the first journal operation in this
     * journal.
     * <p>
     * This page position is written to a {@link JournalHeader} and then the
     * journal is read by reading from the start position and navigating to the
     * continued journal pages via {@link NextOperation} journal operations.
     * 
     * @return The start of the journal.
     */
    public long getJournalStart()
    {
        if (writer.getJournalStart() == 0L)
        {
            writer = writer.extend();
        }
        return writer.getJournalStart();
    }
    
    /**
     * Get the current page position with offset of where the next journal
     * operation will be written or read.
     * 
     * @return The current page position with offset of the journal.
     */
    public long getJournalPosition()
    {
        return writer.getJournalPosition();
    }

    /**
     * Get the set of journal pages used to record this journal.
     * <p>
     * This method is called by mutator rollback to obtain the pages so they can
     * be returned to the interim page pool. During playback, the {@link Player}
     * will track the pages read in a page set of its own, since this in memory
     * journal representation is discarded before journal playback.
     * 
     * 
     * @return The set of journal pages used to record this journal.
     */
    public Set<Long> getJournalPages()
    {
        return writer.getJournalPages();
    }

    /**
     * Write the given journal operation to the journal allocating a new journal
     * page if necessary.
     * 
     * @param operation
     *            The journal operation to write.
     */
    public void write(Operation operation)
    {
        while (!writer.write(operation))
        {
            writer = writer.extend();
        }
    }
    
    /**
     * Reset the journal so that it does not reference any journal pages and all
     * journal pages can be returned to the interim page pool.
     */
    public void reset()
    {
        writer = writer.reset();
    }
}