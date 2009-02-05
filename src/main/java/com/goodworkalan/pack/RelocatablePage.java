package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.goodworkalan.sheaf.Page;
import com.goodworkalan.sheaf.RawPage;
import com.goodworkalan.sheaf.Sheaf;

/**
 * Abstract superclass class for the relocatble page implementations; block
 * pages and journal pages.
 * 
 * @author Alan Gutierrez
 * 
 */
class RelocatablePage extends Page
{
    /**
     * Relocate a page from one position to another writing it out to the file
     * channel of the sheaf associated with the raw page. This method does not
     * use a dirty page map, the page is written immediately.
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
            sheaf.getFileChannel().write(bytes, to);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        rawPage.setPosition(to);
    }
}