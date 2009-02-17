package com.goodworkalan.pack;

// FIXME Comment.
public final class PackException
extends RuntimeException
{
    private static final long serialVersionUID = 20070821L;
    
    public final static int ERROR_BLOCK_PAGE_CORRUPT = 601;

    public final static int ERROR_CORRUPT = 602;

    public final static int ERROR_FILE_NOT_FOUND = 400;

    public final static int ERROR_FILE_SIZE = 503;

    public final static int ERROR_FREED_ADDRESS = 300;

    public final static int ERROR_FREED_STATIC_ADDRESS = 301;

    public final static int ERROR_HEADER_CORRUPT = 600;

    public final static int ERROR_IO_CLOSE = 406;

    public final static int ERROR_IO_FORCE = 405;

    public final static int ERROR_IO_READ = 402;

    public final static int ERROR_IO_SIZE = 403;

    public final static int ERROR_IO_STATIC_PAGES = 407;

    public final static int ERROR_IO_TRUNCATE = 404;

    public final static int ERROR_IO_WRITE = 401;

    public final static int ERROR_SHUTDOWN = 502;

    public final static int ERROR_SIGNATURE = 501;

    private final int code;

    public PackException(int code)
    {
        super(Integer.toString(code));
        this.code = code;
    }

    public PackException(int code, Throwable cause)
    {
        super(Integer.toString(code), cause);
        this.code = code;
    }
    
    public int getCode()
    {
        return code;
    }
}