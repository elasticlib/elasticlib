package store.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.Thread.currentThread;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.Digest;
import store.common.Hash;
import static store.common.IoUtil.copyAndDigest;

/**
 * Represents a test content.
 */
public final class Content {

    private final byte[] bytes;
    private final Digest digest;

    /**
     * Constructor. Loads the actual resource from the classpath.
     *
     * @param filename Resource filename.
     */
    public Content(String filename) {
        try (InputStream inputStream = currentThread().getContextClassLoader().getResourceAsStream(filename);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            digest = copyAndDigest(inputStream, outputStream);
            bytes = outputStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return An input stream on this content.
     */
    public InputStream getInputStream() {
        return new ByteArrayInputStream(bytes);
    }

    /**
     * @return This content as a byte array.
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * @return This content's hash.
     */
    public Hash getHash() {
        return digest.getHash();
    }

    /**
     * @return Info on this content.
     */
    public ContentInfo getInfo() {
        return new ContentInfoBuilder()
                .withLength(digest.getLength())
                .withHash(digest.getHash())
                .computeRevAndBuild();
    }
}
