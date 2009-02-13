package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;

/**
 * Interprets an underlying page as an array of file positions that reference a
 * file position in the user region of the file. A user address references a
 * position in an address page that contains a long value indicating the
 * position of the user block page containing the block in the user region of
 * the file.
 * <p>
 * The address is a long value indicating the actual position of where the file
 * position of the user block page is stored. It is an indirection. To find the
 * position of a user block, we read the long value at the position indicated by
 * the address to find the user block page that contains the user block. We then
 * scan the user block page for the block that contains the address in its
 * address back-reference.
 * <p>
 * Unused addresses are indicated by a zero data position value. If an address
 * is in use, there will be a non-zero position value in the slot.
 * <p>
 * When we allocate a new block, because of isolation, we cannot write out the
 * address of the user block page of the new user block until we are playing
 * back a flushed journal. Thus, during the isolated mutation, we reserve the
 * address position by writing the maximum long value as the value of the page
 * position.
 */
final class AddressPage extends Page
{
    /**
     * A count of free addresses available for reservation on the address
     * page.
     */
    private int freeCount;

    /**
     * Construct an uninitialized address page that is then initialized by
     * calling the {@link #create create} or {@link #load load} methods. The
     * default constructor creates an empty address page that must be
     * initialized before use.
     * <p>
     * All of the page classes have default constructors. This constructor is
     * called by clients of the <code>Sheaf</code> when requesting pages or
     * creating new pages.
     * <p>
     * An uninitialized page of the expected Java class of page is given to the
     * <code>Sheaf</code>. If the page does not exist, the empty, default
     * constructed page is used, if not is ignored and garbage collected. This
     * is a variation on the prototype object construction pattern.
     * 
     * @see com.goodworkalan.sheaf.Sheaf#getPage(long, Class, Page)
     * @see com.goodworkalan.sheaf.Sheaf#setPage(long, Class, Page,
     *      DirtyPageSet, boolean)
     */
    public AddressPage()
    {
    }

    /**
     * Load the address page from the raw page. This method will generate a
     * count of free pages by scanning the raw page at address positions
     * looking for zero, the unallocated value.
     */
    public void load()
    {
        ByteBuffer bytes = getRawPage().getByteBuffer();

        bytes.position(0);

        while (bytes.remaining() > Long.SIZE / Byte.SIZE)
        {
            long position = bytes.getLong();
            if (position == 0L)
            {
                freeCount++;
            }
        }
    }

    /**
     * Create a new address page from the raw page. Initializes by writing
     * out zero values at each address offset in the address page. This
     * method will set the page of the raw page to this address page.
     * 
     * @param dirtyPages
     *            A set of pages that need to be flushed to disk.
     */
    public void create(DirtyPageSet dirtyPages)
    {
        ByteBuffer bytes = getRawPage().getByteBuffer();

        bytes.clear();
        
        while (bytes.remaining() > Long.SIZE / Byte.SIZE)
        {
            bytes.putLong(0L);
            freeCount++;
        }

        dirtyPages.add(getRawPage());
    }
    
    /**
     * Return the count of free addresses, addresses that are neither
     * allocated nor reserved for allocation.
     * 
     * @return The count of free addresses.
     */
    public int getFreeCount()
    {
        synchronized (getRawPage())
        {
            return freeCount;
        }
    }

    /**
     * Reserve an available address from the address page. Reserving an address
     * requires marking it as reserved by using an unlikely file position value
     * - <code>Long.MAX_VALUE</code> - as a reservation value.
     * <p>
     * An address is returned to the poll by setting it to zero. The reservation
     * page is tracked with the dirty page map. It can be released after the
     * dirty page map flushes the reservation page to disk.
     * 
     * @param dirtyPages
     *            A set of pages that need to be flushed to disk.
     * @return A reserved address or 0 if none are available.
     */
    public long reserve(DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            // Get the page buffer.
            
            ByteBuffer bytes = getRawPage().getByteBuffer();
            bytes.clear();

            // Iterate the page buffer looking for a zeroed address that has
            // not been reserved, reserving it and returning it if found.
            
            for (int offset = 0; offset < bytes.capacity(); offset += Pack.ADDRESS_SIZE)
            {
                if (bytes.getLong(offset) == 0L)
                {
                    dirtyPages.add(getRawPage());
                    bytes.putLong(offset, Long.MAX_VALUE);
                    getRawPage().invalidate(offset, Pack.POSITION_SIZE);
                    freeCount--;
                    return getRawPage().getPosition() + offset;
                }
            }

            throw new IllegalStateException();
        }
    }

    /**
     * Set the value of an address position to reference a specified file
     * position. The <code>DirtyPageMap</code> will record the raw pages
     * that are altered by this method.
     * 
     * @param address
     *            The address position to set.
     * @param position
     *            The file position that the address references.
     * @param dirtyPages
     *            A set of pages that need to be flushed to disk.
     */
    public void set(long address, long position, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            int offset = (int) (address - getRawPage().getPosition());
            bytes.putLong(offset, position);
            getRawPage().invalidate(offset, Pack.POSITION_SIZE);
            dirtyPages.add(getRawPage());
        }
    }

    /**
     * Return the page position associated with the address.
     * 
     * @param address
     *            The address.
     * @return The page position associated with the address.
     */
    public long dereference(long address)
    {
        synchronized (getRawPage())
        {
            int offset = (int) (address - getRawPage().getPosition());
            return getRawPage().getByteBuffer().getLong(offset);
        }
    }

    /**
     * Free an address overwriting it with a zero value.
     * 
     * @param address
     *            The address.
     * @param dirtyPages
     *            A set of pages that need to be flushed to disk.
     */
    public void free(long address, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            int offset = (int) (address - getRawPage().getPosition());
            long position = bytes.getLong(offset);
            if (position != 0L)
            {
                bytes.putLong(offset, 0L);
                
                getRawPage().invalidate(offset, Pack.POSITION_SIZE);
                dirtyPages.add(getRawPage());
                
                freeCount++;
            }
        }
    }
    
    public Map<Long, Long> toMap(int skip)
    {
        Map<Long, Long> map = new HashMap<Long, Long>();
        synchronized (getRawPage())
        {
            long position = getRawPage().getPosition();
            ByteBuffer bytes = getRawPage().getByteBuffer();
            bytes.clear();
            for (int offset = skip * Pack.ADDRESS_SIZE; offset < bytes.capacity(); offset += Pack.ADDRESS_SIZE)
            {
                long value = bytes.getLong(offset);
                if (value != 0L && value != Long.MAX_VALUE)
                {
                    map.put(position + offset, value);
                }
            }
        }
        return map;
    }
}