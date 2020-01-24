/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.easymock.EasyMock.createNiceMock;
import static org.junit.Assert.assertThrows;

public class AutoAbortingS3InputStreamTest {
    @Test
    public void drainTest() throws IOException {
        S3ObjectInputStream inputStream = createNiceMock(S3ObjectInputStream.class);
        EasyMock.expect(inputStream.read(EasyMock.anyObject())).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                return 10;
            }
        });
        EasyMock.expect(inputStream.read(EasyMock.anyObject())).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                return 10;
            }
        });

        EasyMock.expect(inputStream.read(EasyMock.anyObject(), EasyMock.anyInt(),
                EasyMock.anyInt())).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                return 90;
            }
        });
        inputStream.close();
        EasyMock.expectLastCall().andVoid();
        EasyMock.replay(inputStream);

        AutoAbortingS3InputStream autoAborting = new AutoAbortingS3InputStream(inputStream, 100, 110);
        int read1 = autoAborting.read(new byte[10]);
        assertEquals(10, read1);
        int read2 = autoAborting.read(new byte[10]);
        assertEquals(10, read2);
        autoAborting.close();
        EasyMock.verify(inputStream);
    }

    @Test
    public void excessiveDrainTest() throws IOException {
        S3ObjectInputStream inputStream = createNiceMock(S3ObjectInputStream.class);
        EasyMock.expect(inputStream.read(EasyMock.anyObject())).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                return 10;
            }
        });
        EasyMock.expect(inputStream.read(EasyMock.anyObject())).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                return 10;
            }
        });

        // abort expected as remaining exceeds auto abort size
        inputStream.abort();
        EasyMock.expectLastCall().andVoid();
        EasyMock.replay(inputStream);

        AutoAbortingS3InputStream autoAborting = new AutoAbortingS3InputStream(inputStream, 100, 300);
        int read1 = autoAborting.read(new byte[10]);
        assertEquals(10, read1);
        int read2 = autoAborting.read(new byte[10]);
        assertEquals(10, read2);
        autoAborting.close();
        EasyMock.verify(inputStream);
    }

    @Test
    public void exceptionOnReadAttemptReread() throws IOException {
        S3ObjectInputStream inputStream = createNiceMock(S3ObjectInputStream.class);
        EasyMock.expect(inputStream.read(EasyMock.anyObject())).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                return 10;
            }
        });

        EasyMock.expect(inputStream.read(EasyMock.anyObject()))
                .andThrow(new IOException("failed to read"));

        // abort expected as remaining exceeds auto abort size
        inputStream.abort();
        EasyMock.expectLastCall().andVoid();
        EasyMock.replay(inputStream);

        AutoAbortingS3InputStream autoAborting = new AutoAbortingS3InputStream(inputStream, 100, 300);
        int read1 = autoAborting.read(new byte[10]);
        assertEquals(10, read1);

        assertThrows(IOException.class, () -> autoAborting.read(new byte[10]));
        assertThrows(IllegalStateException.class, () -> autoAborting.read(new byte[10]));

        autoAborting.close();
        EasyMock.verify(inputStream);
    }

    @Test
    public void abortTest() throws IOException {
        S3ObjectInputStream inputStream = createNiceMock(S3ObjectInputStream.class);
        EasyMock.expect(inputStream.read(EasyMock.anyObject())).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                return 10;
            }
        });
        EasyMock.expect(inputStream.read(EasyMock.anyObject()))
                .andThrow(new IOException("failed to read"));

        // expect abort on input stream, not close
        inputStream.abort();
        EasyMock.expectLastCall().andVoid();
        EasyMock.replay(inputStream);

        AutoAbortingS3InputStream autoAborting = new AutoAbortingS3InputStream(inputStream, 100, 110);
        int read1 = autoAborting.read(new byte[10]);
        assertEquals(10, read1);
        assertThrows(IOException.class, () -> autoAborting.read(new byte[10]));
        autoAborting.close();
        EasyMock.verify(inputStream);
    }
}
