package com.goodworkalan.pack;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

final class CommitMoveTracker extends CompositeMoveTracker
{
    /**
     * A map of interim pages containing new allocations to destination user
     * blocks pages that already contain blocks from other mutators.
     */
    private final MapMoveTracker interimToSharedUserPage;

    /**
     * A map of interim pages containing new allocations to destination user
     * blocks pages that contain no blocks from other mutators.
     */
    private final MapMoveTracker interimToEmptyUserPage;

    private final SortedSet<Long> userFromInterimPages;

    private final SortedSet<Long> addressFromUserPagesToMove;

    private final SetMoveTracker unassignedInterimBlockPages;

    private final SortedMap<Long, Movable> movingUserPageMirrors;

    public CommitMoveTracker(PageMoveTracker pageRecorder, Journal journal,
            MoveNodeMoveTracker moveNodeRecorder)
    {
        this.userFromInterimPages = new TreeSet<Long>();
        this.addressFromUserPagesToMove = new TreeSet<Long>();
        this.movingUserPageMirrors = new TreeMap<Long, Movable>();
        add(unassignedInterimBlockPages = new SetMoveTracker());
        add(pageRecorder);
        add(interimToSharedUserPage = new MapMoveTracker());
        add(interimToEmptyUserPage = new MapMoveTracker());
        add(moveNodeRecorder);
        add(new JournalMoveTracker(journal));
    }

    /**
     * A set of positions of user pages that have been created during the commit
     * by relocating interim pages and expanding the user page region. These
     * pages were interim pages at the start of this commit.
     * 
     * @return A set of positions of newly created user pages.
     */
    public SortedSet<Long> getUserFromInterimPages()
    {
        return userFromInterimPages;
    }

    public SortedMap<Long, Movable> getMovingUserPageMirrors()
    {
        return movingUserPageMirrors;
    }

    public SortedSet<Long> getAddressFromUserPagesToMove()
    {
        return addressFromUserPagesToMove;
    }

    /**
     * Return the set of allocation pages whose blocks have not yet been
     * assigned to a user block page.
     * 
     * @return The set of unassigned interim blocks.
     */
    public SortedSet<Long> getUnassignedInterimBlockPages()
    {
        return unassignedInterimBlockPages;
    }

    /**
     * Return a map of interim pages containing new allocations to destination
     * user blocks pages that are already in use and contain user blocks
     * allocated by other mutators.
     * 
     * @return The map of shared user pages.
     */
    public SortedMap<Long, Movable> getInterimToSharedUserPage()
    {
        return interimToSharedUserPage;
    }

    /**
     * Return a map of interim pages containing new allocations to destination
     * user blocks pages that are not currently in use and contain no blocks
     * from other mutators.
     * 
     * @return The map of empty user pages.
     */
    public SortedMap<Long, Movable> getInterimToEmptyUserPage()
    {
        return interimToEmptyUserPage;
    }
}