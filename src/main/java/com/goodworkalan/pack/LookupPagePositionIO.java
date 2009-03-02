package com.goodworkalan.pack;

import java.util.List;

import com.goodworkalan.sheaf.DirtyPageSet;

/**
 * An I/O strategy to read and write the page position of a lookup page used to
 * allocate lookup blocks for a block size specified by an index into a list of
 * block sizes. This is a strategy to manage pages used to allocate new blocks
 * for a lookup page pool. A lookup page pool will allocate blocks of the sizes
 * specified in the block size list.
 * <p>
 * When the first page position is added to the lookup page pool, a block of the
 * size specified by the last value in the block size is allocated. When that
 * block is filled, a block is allocated that is the size of the size indicated
 * by the second to last size in the size list. When the first size in the size
 * list is reached, any subsequent allocations will be the size of the first
 * size in the size list.
 * <p>
 * This allows for an allocation strategy where larger and larger blocks are
 * allocated for a lookup page pool, until a full page block size is reached.
 * <p>
 * This class is used to read and write the positions of the block allocation
 * pages for each block size in the block size list. 
 * 
 * @author Alan Gutierrez
 */
public interface LookupPagePositionIO
{
    /**
     * Write the given page position of block allocation page for the block size
     * in the block size list at the given index. The dirty page set is used to
     * track any pages that are allocated or update by this method.
     * 
     * @param blockSizeIndex
     *            The index of the block size in the block size list.
     * @param position
     *            The page position of the block allocation page for the block
     *            size.
     * @param dirtyPages
     *            The dirty page set.
     * @return The block allocation page for the block size.
     */
    public void write(int blockSizeIndex, long position, DirtyPageSet dirtyPages);

    /**
     * Read the page position of the block allocation page for the block size in
     * the block size list at the given index.
     * 
     * @param blockSizeIndex
     *            The index of the block size in the block size list.
     * @return The page position of the block allocation page for the block
     *         size.
     */
    public long read(int blockSizeIndex);

    /**
     * Get the list of block sizes. See the class documentation for a
     * description of how the block size list is used to choose a block
     * allocation page.
     * 
     * @return The block size list.
     */
    public List<Integer> getBlockSizes();
}
