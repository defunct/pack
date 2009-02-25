package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * A general purpose exception that indicates that an error occurred in one of
 * the classes in the pack package.
 * <p>
 * The exception is created with an error code. A client programmer can use the
 * error code in a switch statement to respond to specific error conditions.
 * 
 * @author Alan Gutierrez
 */
public final class PackException
extends RuntimeException
{
    /** The serial version id. */
    private static final long serialVersionUID = 20070821L;
    
    /** A block page is corrupt. */ 
    public final static int ERROR_BLOCK_PAGE_CORRUPT = 601;

    /** Illegal attempt to dereference a freed address. */
    public final static int ERROR_FREED_ADDRESS = 300;

    /** Illegal attempt to free a static named address. */
    public final static int ERROR_FREED_STATIC_ADDRESS = 301;
    
    /**
     * Illegal attempt to dereference an page position that is not an address
     * page position.
     */
    public final static int ERROR_INVALID_ADDRESS = 302;

    /** The file header is corrupt. */
    public final static int ERROR_HEADER_CORRUPT = 600;

    /** An error occurred while closing the file channel. */
    public final static int ERROR_IO_CLOSE = 406;

    /** An error occurred while forcing the file channel to write to disk. */
    public final static int ERROR_IO_FORCE = 405;

    /** An error occurred while reading the file channel. */
    public final static int ERROR_IO_READ = 402;

    /** An error occurred while getting the file channel size. */
    public final static int ERROR_IO_SIZE = 403;

    /** An error occurred while truncating the file channel. */
    public final static int ERROR_IO_TRUNCATE = 404;

    /** An error occurred while writing to the file channel. */
    public final static int ERROR_IO_WRITE = 401;

    /** The shutdown flag is corrupt. */
    public final static int ERROR_SHUTDOWN_ = 502;

    /** The flag indicating that the file is a {@link Pack} file is corrupt. */
    public final static int ERROR_SIGNATURE = 501;

    /** A list of arguments to the formatted error message. */
    private final List<Object> arguments = new ArrayList<Object>(); 
    
    /** The error code. */
    private final int code;

    /**
     * Create an exception with the given error code.
     * 
     * @param code
     *            The error code.
     */
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

    /**
     * Get the error code.
     * 
     * @return The error code.
     */
    public int getCode()
    {
        return code;
    }

    /**
     * Add an argument to the list of arguments to provide the formatted error
     * message associated with the error code.
     * 
     * @param argument
     *            The format argument.
     * @return This sheaf exception for chained invocation of add.
     */
    public PackException add(Object argument)
    {
        arguments.add(argument);
        return this;
    }

    /**
     * Create an detail message from the error message format associated with
     * the error code and the format arguments.
     * 
     * @return The exception message.
     */
    @Override
    public String getMessage()
    {
        String key = Integer.toString(code);
        ResourceBundle exceptions = ResourceBundle.getBundle("com.goodworkalan.sheaf.exceptions");
        String format;
        try
        {
            format = exceptions.getString(key);
        }
        catch (MissingResourceException e)
        {
            return key;
        }
        try
        {
            return String.format(format, arguments.toArray());
        }
        catch (Throwable e)
        {
            throw new Error(key, e);
        }
    }
}