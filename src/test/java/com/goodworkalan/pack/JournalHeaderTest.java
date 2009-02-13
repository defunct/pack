package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.mockito.Mockito.*;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.goodworkalan.pack.JournalHeader;
import com.goodworkalan.sheaf.SheafException;

public class JournalHeaderTest
{
    @Test(expectedExceptions=SheafException.class)
    public void ioException() throws IOException
    {
        FileChannel fileChannel = mock(FileChannel.class);
        when(fileChannel.write(Mockito.<ByteBuffer>anyObject(), anyLong())).thenThrow(new IOException());
        new JournalHeader(ByteBuffer.allocate(1), 1, new Object()).write(fileChannel);
    }
}
