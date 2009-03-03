package com.goodworkalan.pack;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Manages a linked list of pages that are employed as arrays of page position
 * references. A reference pool can only grow, it cannot shrink. New reference
 * pages are linked from a head page position stored in the file header. The
 * abstract methods {@link #getHeaderField(Header)} and
 * {@link #setHeaderField(Header, long)} are defined to get and set the page
 * position in file header.
 * 
 * @author Alan Gutierrez
 */
abstract class ReferencePool
{
    /** The page manager. */
    private final Sheaf sheaf;
    
    /** The file header. */
    private final Header header;
    
    /** The boundary between address pages and user pages. */
    private final AddressBoundary addressBoundary;
    
    /** The interim page pool to use to allocate new reference pages. */
    private final InterimPagePool interimPagePool;
    
    /**
     * A linked list of reference pages. Note that the pages are maintained as
     * singly linked list of pages on disk as well.
     */
    private final LinkedList<Long> referencePages;

    /**
     * Create a reference pool that reads the existing linked list of reference
     * pages from the given page manager starting from linked list referenced in
     * the given file header. The given user boundary is used to locate
     * reference pages that were moved by the expansion of the address region.
     * 
     * @param sheaf
     *            The page manager.
     * @param header
     *            The file header.
     * @param addressBoundary
     *            The boundary between address pages and user pages.
     * @param interimPagePool
     *            The interim page pool to use to allocate new reference pages.
     */
    public ReferencePool(Sheaf sheaf, Header header, AddressBoundary addressBoundary, InterimPagePool interimPagePool)
    {
        referencePages = new LinkedList<Long>();
        long position = getHeaderField(header);
        while (position != Long.MIN_VALUE)
        {
            referencePages.add(position);
            AddressPage references = addressBoundary.load(position, AddressPage.class, new AddressPage());
            position = references.dereference(position);
        }
        this.sheaf = sheaf;
        this.addressBoundary = addressBoundary;
        this.header = header;
        this.interimPagePool = interimPagePool;
    }

    /**
     * Create a map of the references page positions with offset to the page
     * position or address that they reference. References that contain a zero
     * value or the reference reservation value are excluded from the map. The
     * given user boundary is used to locate reference pages that were moved by
     * the expansion of the address region.
     * 
     * @param sheaf
     *            The page manager.
     * @param userBoundary
     *            The boundary between address pages and user pages.
     * @return A map of non-zero, non-reserved reference page positions with
     *         offset to the page position or address that they reference.
     */
    public Map<Long, Long> toMap(Sheaf sheaf, AddressBoundary userBoundary)
    {
        Map<Long, Long> map = new HashMap<Long, Long>();
        for (long position : referencePages)
        {
            AddressPage references = userBoundary.load(position, AddressPage.class, new AddressPage());
            map.putAll(references.toMap(1));
        }
        return map;
    }

    /**
     * Get the page position from the header of the first reference page in a
     * linked list of reference pages.
     * 
     * @param header
     *            The file header.
     * @return The page position from the header of the first page in a linked
     *         list of reference pages.
     */
    protected abstract long getHeaderField(Header header);

    /**
     * Get the page position from the header of the first reference page in a
     * linked list of reference pages.
     * 
     * @param header
     *            The file header.
     * @param position
     *            The page position from the header of the first page in a
     *            linked list of reference pages.
     */
    protected abstract void setHeaderField(Header header, long position);

    /**
     * Reserve a reference from one of the reference pages in the linked list of
     * reference pages. The given user boundary is used to locate reference
     * pages that were moved by the expansion of the address region.
     * 
     * @param sheaf
     *            The page manager.
     * @param userBoundary
     *            The boundary between address pages and user pages.
     * @param dirtyPages
     *            The dirty page set.
     * @return A reserved page reference.
     */
    private long reserve(Sheaf sheaf, AddressBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        long reference = 0L;
        int size = referencePages.size();
        for (int i = 0; i < size; i++)
        {
            long position = referencePages.getFirst();
            AddressPage references = userBoundary.load(position, AddressPage.class, new AddressPage());
            synchronized (references.getRawPage())
            {
                if (references.getFreeCount() != 0)
                {
                    reference = references.reserve(dirtyPages);
                    break;
                }
            }
            referencePages.addLast(referencePages.removeFirst());
        }
        return reference;
    }

    /**
     * Allocate a reference from the reference pool. If there are no free
     * reference positions available in any of the reference pages, a new
     * reference page is created and linked to the header reference.
     * 
     * @param dirtyPages
     *            The dirty page set.
     * @return An reference from the reference pool.
     */
    public synchronized long allocate(DirtyPageSet dirtyPages) 
    {
        long reference = reserve(sheaf, addressBoundary, dirtyPages);
        if (reference == 0L)
        {
            DirtyPageSet allocDirtyPages = new DirtyPageSet();
            long position = interimPagePool.newBlankInterimPage(true);
            AddressPage references = sheaf.setPage(position, AddressPage.class, new AddressPage(), allocDirtyPages);
            synchronized (header)
            {
                references.set(position, getHeaderField(header), dirtyPages);
                allocDirtyPages.flush();
                try
                {
                    sheaf.getFileChannel().force(false);
                }
                catch (IOException e)
                {
                    throw new PackException(PackException.ERROR_IO_FORCE, e);
                }
                setHeaderField(header, position);
                try
                {
                    header.write(sheaf.getFileChannel(), 0);
                }
                catch (IOException e)
                {
                    throw new PackException(PackException.ERROR_IO_WRITE, e);
                }
                try
                {
                    sheaf.getFileChannel().force(false);
                }
                catch (IOException e)
                {
                    throw new PackException(PackException.ERROR_IO_FORCE, e);
                }
                referencePages.addFirst(position);
            }
            
            reference = reserve(sheaf, addressBoundary, dirtyPages);
        }
        return reference;
    }
}
