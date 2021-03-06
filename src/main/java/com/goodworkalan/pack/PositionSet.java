package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.goodworkalan.region.BasicRegion;
import com.goodworkalan.region.Cleanable;
import com.goodworkalan.region.NullCleanable;
import com.goodworkalan.region.Region;

/**
 * Maintains a set of allocated and free positions that reference a position on
 * range of the underlying file as a set of position. Used to maintain a set of
 * available slots in which to write the first address of a chain of journal
 * pages.
 * 
 * @author Alan Gutierrez
 */
final class PositionSet
{
    /** Booleans indicating whether or not the page is free. */
    private final boolean[] reserved;

    /** The first position. */
    private final long position;

    /**
     * Create a position set begining at the offset position and continuing for
     * count positions. The size of this array in the file is the size of a long
     * by the count. Remember that the positions store positions.
     * 
     * @param position
     *            The position offset of the first position value.
     * @param count
     *            The number of position values kept at the position offset.
     */
    public PositionSet(long position, int count)
    {
        this.position = position;
        this.reserved = new boolean[count];
    }

    /**
     * Get the number of position values at the offset position.
     * 
     * @return The number of position values at the offset position.
     */
    public int getCapacity()
    {
        return reserved.length;
    }

    /**
     * Allocate an available position in which to store a position value from
     * the range covered by this position set waiting if the set is empty. This
     * method is synchronized so that the pointer set can be drawn from by
     * different mutators in different threads. If there are no positions
     * available, this method will wait until a position is freed by another
     * thread.
     * 
     * @return A position in which to store a position value from the range
     *         covered by this position set.
     */
    public Region allocate()
    {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Pack.LONG_SIZE);
        Lock lock =  new ReentrantLock();
        Cleanable cleanable = new NullCleanable(Pack.LONG_SIZE);
        synchronized (this)
        {
            Region segment = null;
            for (;;)
            {
                for (int i = 0; i < reserved.length && segment == null; i++)
                {
                    if (!reserved[i])
                    {
                        reserved[i] = true;
                        segment = new BasicRegion(position + i * Pack.LONG_SIZE, byteBuffer, lock, cleanable);
                    }
                }
                if (segment == null)
                {
                    try
                    {
                        wait();
                    }
                    catch (InterruptedException e)
                    {
                    }
                }
                else
                {
                    break;
                }
            }
            return segment;
        }
    }

    /**
     * Free a position in the range covered by this position set notifying any
     * other threads waiting for a position that one is available.
     * 
     * @param pointer
     *            A structure containing the position the free.
     */
    public synchronized void free(Region pointer)
    {
        int offset = (int) (pointer.getPosition() - position) / Pack.LONG_SIZE;
        reserved[offset] = false;
        notify();
    }
}
