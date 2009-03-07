package com.goodworkalan.pack;

import com.goodworkalan.region.Header;
import com.goodworkalan.region.HeaderBuilder;

class Housekeeping
{
    /**
     * The header key of the signature at the very start of the file that indicates that
     * this might be a <code>Pack</code> file.
     */
    final static int SIGNATURE = 1;

    /**
     * The header key of the flag indicating a soft shutdown or a hard shutdown.
     * The value of this flag is one of {@link Pack#SOFT_SHUTDOWN} or
     * {@link Pack#HARD_SHUTDOWN}.
     */
    final static int SHUTDOWN = 2;
    
    /**  The header key of the size of a page in the file. */
    final static int PAGE_SIZE = 3;
    
    /**
     * The header key of the alignment to which all block allocations are
     * rounded.
     */
    final static int ALIGNMENT = 4;

    /**
     * The header key of the count of journal headers in the header of the file.
     */
    final static int JOURNAL_COUNT = 5;
    
    /** The header key of the count of static blocks. */
    final static int STATIC_BLOCK_COUNT = 6;

    /**
     * The header key of the size of the pack file header including the set of
     * named static pages and the journal headers.
     */
    final static int HEADER_SIZE = 7;

    /**
     * The header key of the minimum number of address pages with addresses
     * available for allocation.
     */
    final static int ADDRESS_PAGE_POOL_SIZE = 8;

    /**
     * The header key of the address region boundary of the pack file. The
     * address region boundary is the position of the first user page, less the
     * file header offset.
     */
    final static int ADDRESS_BOUNDARY = 9;

    /**
     * The header key of the address of the lookup page pool used to track
     * address pages with at least one address remaining for allocation.
     */
    final static int ADDRESS_LOOKUP_PAGE_POOL = 10;

    /**
     * The header key of the file position of the first temporary block node in
     * a linked list of temporary block nodes.
     */
    final static int FIRST_TEMPORARY_RESOURCE_PAGE = 11;

    /**
     * The header key of the page position of the table of block pages ordered
     * by bytes available for block allocation.
     */
    final static int BY_REMAINING_TABLE = 12;

    static Header<Integer> newHeader()
    {
        HeaderBuilder<Integer> newHeader = new HeaderBuilder<Integer>();
        newHeader.addField(SIGNATURE, Pack.LONG_SIZE);
        newHeader.addField(SHUTDOWN, Pack.INT_SIZE);
        newHeader.addField(PAGE_SIZE, Pack.INT_SIZE);
        newHeader.addField(ALIGNMENT, Pack.INT_SIZE);
        newHeader.addField(JOURNAL_COUNT, Pack.INT_SIZE);
        newHeader.addField(STATIC_BLOCK_COUNT, Pack.INT_SIZE);
        newHeader.addField(HEADER_SIZE, Pack.INT_SIZE);
        newHeader.addField(ADDRESS_PAGE_POOL_SIZE, Pack.INT_SIZE);
        newHeader.addField(ADDRESS_BOUNDARY, Pack.LONG_SIZE);
        newHeader.addField(ADDRESS_LOOKUP_PAGE_POOL, Pack.LONG_SIZE);
        newHeader.addField(FIRST_TEMPORARY_RESOURCE_PAGE, Pack.LONG_SIZE);
        newHeader.addField(BY_REMAINING_TABLE, Pack.LONG_SIZE);
        return newHeader.newHeader(0L);
    }
    
    /**
     * Get the position where the static block map of URIs to static address is
     * stored.
     * 
     * @return The position where the static block map is stored.
     */
    public static long getStaticBlockMapStart(Header<Integer> header, int journalCount)
    {
        return header.getLength() + journalCount * Pack.LONG_SIZE;
    }
}
