package com.goodworkalan.pack;

import com.goodworkalan.sheaf.Page;

/**
 * Manages a bit set indicating which pages in the non-address region of the
 * file are in use as block pages or other user pages, and which pages are
 * unused. This page is used during a survey after recovery to recreate the free
 * page housekeeping file structures.
 * 
 * @author Alan Gutierrez
 */
public class PageUsePage extends Page
{
}
