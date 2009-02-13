package com.goodworkalan.pack;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.Checksum;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * A null journal writer used to initiated a linked list of journal pages.
 * <p>
 * The {@link NullJournalWriter#write(Operation) write} method of implementation
 * of journal writer always returns false so that the caller will in turn call
 * the {@link #extend() extend} method. The <code>extend</code> method will
 * create the first journal page in a linked list of journal pages. It will also
 * create a movable reference to the first page which is passed along to the the
 * new journal writers created by the {@link JournalWriter#extend() extend}
 * method of a {@link JournalWriter}.
 * 
 * @author Alan Gutierrez
 */
class NullJournalWriter
extends JournalWriter
{
    /**
     * Create a null journal writer that initiate a new linked list of journal
     * pages.
     * <p>
     * The given move node recorder is used by reset to create a
     * {@link NullJournalWriter} which in turn uses it to create a movable
     * reference to the first page of the linked list of journal pages.
     * <p>
     * New page allocations are added to the given page move tracker.
     * 
     * @param sheaf
     *            The sheaf from which to allocate pages.
     * @param interimPagePool
     *            The interim page pool from which to allocate pages.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    public NullJournalWriter(Sheaf sheaf, InterimPagePool interimPagePool, Checksum adler32, DirtyPageSet dirtyPages)
    {
        super(sheaf, interimPagePool, adler32, 0L, null, null, dirtyPages);
    }

    /**
     * A write operation that always returns false to force the creation of a
     * new linked list of journal pages.
     * 
     * @return False to force the call to {@link extend} to create a new liked
     *         list of journal pages.
     */
    public boolean write(Operation operation)
    {
        return false;
    }

    @Override
    public Set<Long> getJournalPages()
    {
        return Collections.<Long>emptySet();
    }

    /**
     * Create a new journal writer that will write to the first page of a linked
     * list of journal pages.
     * 
     * @return A new linked list of journal pages.
     */
    public JournalWriter extend()
    {
        Set<Long> journalPages = new HashSet<Long>();
        JournalPage journal = interimPagePool.newInterimPage(sheaf, JournalPage.class, new JournalPage(), dirtyPages, false);
        journal.writeChecksum(checksum);
        journalPages.add(journal.getRawPage().getPosition());
        return new JournalWriter(sheaf, interimPagePool, checksum, journal.getJournalPosition(), journal, journalPages, dirtyPages);
    }
}