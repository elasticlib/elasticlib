package store.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.Thread.currentThread;
import store.common.ContentInfo;
import static store.common.ContentInfo.contentInfo;
import store.common.Digest;
import store.common.Hash;
import static store.common.IoUtil.copyAndDigest;

public final class Content {

    private final byte[] bytes;
    private final Digest digest;

    public Content(String filename) {
        try (InputStream inputStream = currentThread().getContextClassLoader().getResourceAsStream(filename);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            digest = copyAndDigest(inputStream, outputStream);
            bytes = outputStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputStream getInputStream() {
        return new ByteArrayInputStream(bytes);
    }

    public byte[] getBytes() {
        return bytes;
    }

    public Hash getHash() {
        return digest.getHash();
    }

    public ContentInfo getInfo() {
        return contentInfo()
                .withLength(digest.getLength())
                .withHash(digest.getHash())
                .build();
    }
}
