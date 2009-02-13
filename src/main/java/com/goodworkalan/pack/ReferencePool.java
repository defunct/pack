package com.goodworkalan.pack;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Sheaf;

abstract class ReferencePool
{
    private final LinkedList<Long> referencePages;
    
    public ReferencePool(Sheaf sheaf, UserBoundary userBoundary, Header header)
    {
        referencePages = new LinkedList<Long>();
        long position = getHeaderField(header);
        while (position != Long.MIN_VALUE)
        {
            referencePages.add(position);
            AddressPage references = userBoundary.load(sheaf, position, AddressPage.class, new AddressPage());
            position = references.dereference(position);
        }
    }
    
    public Map<Long, Long> toMap(Sheaf sheaf, UserBoundary userBoundary)
    {
        Map<Long, Long> map = new HashMap<Long, Long>();
        for (long position : referencePages)
        {
            AddressPage references = userBoundary.load(sheaf, position, AddressPage.class, new AddressPage());
            map.putAll(references.toMap(1));
        }
        return map;
    }
    
    protected abstract long getHeaderField(Header header);

    protected abstract void setHeaderField(Header header, long position);
    
    private long reserve(Sheaf sheaf, UserBoundary userBoundary, DirtyPageSet dirtyPages)
    {
        long reference = 0L;
        int size = referencePages.size();
        for (int i = 0; i < size; i++)
        {
            long position = referencePages.getFirst();
            AddressPage references = userBoundary.load(sheaf, position, AddressPage.class, new AddressPage());
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

    public synchronized long allocate(Sheaf sheaf, Header header, UserBoundary userBoundary, InterimPagePool interimPagePool, DirtyPageSet dirtyPages) 
    {
        long reference = reserve(sheaf, userBoundary, dirtyPages);
        if (reference == 0L)
        {
            DirtyPageSet allocDirtyPages = new DirtyPageSet(16);
            long position = interimPagePool.newBlankInterimPage(sheaf, true);
            AddressPage references = sheaf.setPage(position, AddressPage.class, new AddressPage(), allocDirtyPages);
            synchronized (header)
            {
                references.set(position, getHeaderField(header), dirtyPages);
                setHeaderField(header, position);
                allocDirtyPages.flush();
                try
                {
                    header.write(sheaf.getFileChannel(), 0);
                }
                catch (IOException e)
                {
                    throw new PackException(Pack.ERROR_IO_WRITE, e);
                }
                try
                {
                    sheaf.getFileChannel().force(false);
                }
                catch (IOException e)
                {
                    throw new PackException(Pack.ERROR_IO_FORCE, e);
                }
                referencePages.addFirst(position);
            }
            
            reference = reserve(sheaf, userBoundary, dirtyPages);
        }
        return reference;
    }
}
