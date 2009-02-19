package com.goodworkalan.pack;

// TODO Document.
// TODO Add message generation.
public final class PackException
extends RuntimeException
{
    /** The serial version id. */
    private static final long serialVersionUID = 20070821L;
    
    // TODO Document.
    public final static int ERROR_BLOCK_PAGE_CORRUPT = 601;

    // TODO Document.
    public final static int ERROR_CORRUPT = 602;

    // TODO Document.
    public final static int ERROR_FILE_NOT_FOUND = 400;

    // TODO Document.
    public final static int ERROR_FILE_SIZE = 503;

    // TODO Document.
    public final static int ERROR_FREED_ADDRESS = 300;

    // TODO Document.
    public final static int ERROR_FREED_STATIC_ADDRESS = 301;

    // TODO Document.
    public final static int ERROR_HEADER_CORRUPT = 600;

    // TODO Document.
    public final static int ERROR_IO_CLOSE = 406;

    // TODO Document.
    public final static int ERROR_IO_FORCE = 405;

    // TODO Document.
    public final static int ERROR_IO_READ = 402;

    // TODO Document.
    public final static int ERROR_IO_SIZE = 403;

    // TODO Document.
    public final static int ERROR_IO_STATIC_PAGES = 407;

    // TODO Document.
    public final static int ERROR_IO_TRUNCATE = 404;

    // TODO Document.
    public final static int ERROR_IO_WRITE = 401;

    // TODO Document.
    public final static int ERROR_SHUTDOWN = 502;

    // TODO Document.
    public final static int ERROR_SIGNATURE = 501;

    // TODO Document.
    private final int code;

    // TODO Document.
    public PackException(int code)
    {
        super(Integer.toString(code));
        this.code = code;
    }

    // TODO Document.
    public PackException(int code, Throwable cause)
    {
        super(Integer.toString(code), cause);
        this.code = code;
    }
    
    // TODO Document.
    public int getCode()
    {
        return code;
    }
}