/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream wrapper which decides to close or abort an S3ObjectInputStream based on
 * the remaining data left in the stream. This approach is adapted from Hadoop's S3a logic.
 */
public class AutoAbortingS3InputStream extends InputStream {
    private final S3ObjectInputStream innerInputStream;
    private long bytesRead = 0;
    private long totalBytes;
    private final long autoAbortSize;

    AutoAbortingS3InputStream(S3ObjectInputStream innerInputStream,
                              long autoAbortSize,
                              long totalBytes) {
        this.innerInputStream = innerInputStream;
        this.autoAbortSize = autoAbortSize;
        this.totalBytes = totalBytes;
    }

    @Override
    public int read() throws IOException {
        int read = innerInputStream.read();
        bytesRead += read;
        return read;
    }

    @Override
    public int read(byte[] b) throws IOException {
        int read = innerInputStream.read(b);
        bytesRead += read;
        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = innerInputStream.read(b, off, len);
        bytesRead += read;
        return read;
    }

    private long remainingBytes() {
        return totalBytes - bytesRead;
    }

    @Override
    public void close() {
        // This use a strategy taken from Hadoop's S3a. If there are over autoAbortSize bytes, we
        // choose to abort the connection. If we are under autoAbortSize, we read the remaining
        // bytes. This is to be sympathetic to the backing connection pool used by the AmazonS3
        // client. InputStreams which are not aborted allow for connection reuse which helps latency
        // and avoids authorization costs from connection setup.
        // https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AInputStream.java#L521
        boolean shouldAbort = remainingBytes() > autoAbortSize;
        if (shouldAbort) {
            innerInputStream.abort();
        } else {
            try {
                byte[] skipBuf = new byte[1024];
                while (innerInputStream.read(skipBuf, 0, skipBuf.length) > 0) { }
                innerInputStream.close();
            } catch (final IOException ignored) { // If we fail to drain the InputStream, abort it.
                innerInputStream.abort();
            }
        }
    }

    @Override
    public int available() throws IOException {
        return innerInputStream.available();
    }

    @Override
    public long skip(long n) throws IOException {
        long skipped = innerInputStream.skip(n);
        bytesRead += skipped;
        return skipped;
    }
}
