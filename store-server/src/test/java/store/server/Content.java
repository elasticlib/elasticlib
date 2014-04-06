package store.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.Thread.currentThread;
import java.util.SortedSet;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree;
import store.common.Digest;
import store.common.Hash;
import static store.common.IoUtil.copyAndDigest;

/**
 * Represents a test content.
 */
public final class Content {

    private final byte[] bytes;
    private final ContentInfo info;
    private final ContentInfoTree tree;

    /**
     * Constructor. Loads the actual resource from the classpath.
     *
     * @param filename Resource filename.
     */
    public Content(String filename) {
        try (InputStream inputStream = currentThread().getContextClassLoader().getResourceAsStream(filename);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            Digest digest = copyAndDigest(inputStream, outputStream);

            bytes = outputStream.toByteArray();
            info = new ContentInfoBuilder()
                    .withLength(digest.getLength())
                    .withContent(digest.getHash())
                    .computeRevisionAndBuild();

            tree = new ContentInfoTree.ContentInfoTreeBuilder().add(info).build();

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
        return tree.getContent();
    }

    /**
     * @return Head of a the tree associated with this content.
     */
    public SortedSet<Hash> getHead() {
        return tree.getHead();
    }

    /**
     * @return Info on this content.
     */
    public ContentInfo getInfo() {
        return info;
    }

    /**
     * @return A tree containing the info revision of this content.
     */
    public ContentInfoTree getTree() {
        return tree;
    }
}
