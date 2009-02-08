package com.goodworkalan.pack;

public class UserPagePool
{
    private final int alignment;
    
    /**
     * Set of empty user pages. This set is checked for empty pages to store
     * allocations during commit. To reuse an empty user page, the page is
     * removed from the set, so that no other mutator will attempt to use it to
     * write block allocations. Once the commit is complete, all user pages with
     * space remaining are added to the free page by size table, or the free
     * page set if the user page is completely empty.
     */
    private final FreeSet emptyUserPages;
    
    /**
     * A table that orders user pages with block space available by the size of
     * bytes remaining. During commit, this table is checked for user pages that
     * can accommodate block allocations. To use user page, the page is removed
     * from the table, so that no other mutator will attempt to use it to write
     * block allocations. Once the commit is complete, all user pages with space
     * remaining are added to the free page by size table, or the free page set
     * if the user page is completely empty.
     */
    private final ByRemainingTable freePageBySize;

    public UserPagePool(int pageSize, int alignment)
    {
        this.alignment = alignment;
        this.freePageBySize = new ByRemainingTable(pageSize, alignment);
        this.emptyUserPages = new FreeSet();
    }

    /**
     * Return the set of completely empty user pages available for block
     * allocation. The set returned is a class that not only contains the set of
     * pages available, but will also prevent a page from being returned to the
     * set of free pages, if that page is midst of relocation.
     * <p>
     * A user page is used by one mutator commit at a time. Removing the page
     * from this table prevents it from being used by another commit.
     * <p>
     * Removing a page from this set, prevents it from being used by an
     * destination for allocations or writes. Removing a page from the available
     * pages sets is the first step in relocating a page.
     * 
     * @return The set of free user pages.
     */
    public FreeSet getEmptyUserPages()
    {
        return emptyUserPages;
    }

    /**
     * Return a best fit lookup table of user pages with space remaining for
     * block allocation. This lookup table is used to find destinations for
     * newly allocated user blocks during commit.
     * <p>
     * A user page is used by one mutator commit at a time. Removing the page
     * from this table prevents it from being used by another commit.
     * <p>
     * Removing a page from this set, prevents it from being used by an
     * allocation. Removing a page from the available pages sets is the first
     * step in relocating a page.
     * <p>
     * Note that here is no analgous list of free interim pages by size, since
     * interim block pages are not shared between mutators and they are
     * completely reclaimed at the end of a mutation.
     * 
     * @return The best fit lookup table of user pages.
     */
    public ByRemainingTable getFreePageBySize()
    {
        return freePageBySize;
    }

    /**
     * Return a user page to the free page accounting, if the page has any 
     * space remaining for blocks. If the block page is empty, it is added
     * to the set of empty user pages. If it has block space remaining that
     * is greater than the alignment, then it is added to by size lookup table.
     * <p>
     * TODO Pull this out and create a pool.
     * 
     * @param userPage The user block page.
     */
    public void returnUserPage(UserPage userPage)
    {
        if (userPage.getBlockCount() == 0)
        {
            emptyUserPages.free(userPage.getRawPage().getPosition());
        }
        else if (userPage.getRemaining() > alignment)
        {
            getFreePageBySize().add(userPage);
        }
    }
}