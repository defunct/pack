package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.DirtyPageSet;
import com.goodworkalan.sheaf.Page;
import com.goodworkalan.sheaf.RawPage;
import com.goodworkalan.sheaf.Sheaf;

class RelocatablePage
implements Page
{
    private RawPage rawPage;
    
    public void create(RawPage rawPage, DirtyPageSet dirtyPages)
    {
        this.rawPage = rawPage;
        rawPage.setPage(this);
    }

    public void load(RawPage rawPage)
    {
        this.rawPage = rawPage;
        rawPage.setPage(this);
    }
    
    public RawPage getRawPage()
    {
        return rawPage;
    }
    
    /**
     * Relocate a page from one position to another writing it out
     * immediately. This method does not use a dirty page map, the page
     * is written immediately.
     * 
     * @param to
     *            The position where the page will be relocated.
     */
    public void relocate(long to)
    {
        RawPage rawPage = getRawPage();
        Sheaf sheaf = rawPage.getSheaf();
        ByteBuffer bytes = rawPage.getByteBuffer();
        bytes.clear();
        try
        {
            sheaf.getDisk().write(sheaf.getFileChannel(), bytes, to);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        rawPage.setPosition(to);
    }
}