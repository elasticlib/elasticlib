package store.server;

import static com.google.common.collect.Iterables.getOnlyElement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.Thread.currentThread;
import java.util.SortedSet;
import store.common.hash.Digest;
import store.common.hash.Hash;
import static store.common.metadata.Properties.Common.CONTENT_TYPE;
import static store.common.metadata.Properties.Common.FILE_NAME;
import store.common.model.ContentInfo;
import store.common.model.ContentInfo.ContentInfoBuilder;
import store.common.model.ContentInfoTree;
import store.common.model.ContentInfoTree.ContentInfoTreeBuilder;
import static store.common.util.IoUtil.copyAndDigest;
import store.common.value.Value;

/**
 * Represents a test content, with its metadata.
 */
public final class TestContent {

    private final byte[] bytes;
    private final ContentInfoTree tree;

    private TestContent(byte[] bytes, ContentInfoTree tree) {
        this.bytes = bytes;
        this.tree = tree;
    }

    /**
     * Builds a new test content by loading an actual resource from the classpath.
     *
     * @param filename Resource filename.
     * @param contentType Resource content type.
     * @return A content on this resource.
     */
    public static TestContent of(String filename, String contentType) {
        try (InputStream inputStream = currentThread().getContextClassLoader().getResourceAsStream(filename);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            Digest digest = copyAndDigest(inputStream, outputStream);

            ContentInfo info = new ContentInfoBuilder()
                    .withLength(digest.getLength())
                    .withContent(digest.getHash())
                    .with(FILE_NAME.key(), Value.of(filename))
                    .with(CONTENT_TYPE.key(), Value.of(contentType))
                    .computeRevisionAndBuild();

            return new TestContent(outputStream.toByteArray(),
                                   new ContentInfoTreeBuilder().add(info).build());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build a new test content instance with same bytes as this one and a new metadata revision obtained by adding
     * supplied entry.
     *
     * @param key Metadata entry key.
     * @param value Metadata entry value.
     * @return A new TestContent instance.
     */
    public TestContent add(String key, Value value) {
        ContentInfo head = getInfo();
        return new TestContent(bytes, tree.add(new ContentInfoBuilder()
                .withParent(head.getRevision())
                .withLength(head.getLength())
                .withContent(head.getContent())
                .withMetadata(head.getMetadata())
                .with(key, value)
                .computeRevisionAndBuild()));
    }

    /**
     * Build a new test content instance with same bytes as this one and a new metadata revision obtained by adding
     * supplied entry.
     *
     * @return A new TestContent instance.
     */
    public TestContent delete() {
        ContentInfo head = getInfo();
        return new TestContent(bytes, tree.add(new ContentInfoBuilder()
                .withParent(head.getRevision())
                .withLength(head.getLength())
                .withContent(head.getContent())
                .withDeleted(true)
                .computeRevisionAndBuild()));
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
     * @return This content's filename.
     */
    public String filename() {
        return getInfo().getMetadata().get(FILE_NAME.key()).asString();
    }

    /**
     * @return This content's type.
     */
    public String contentType() {
        return getInfo().getMetadata().get(CONTENT_TYPE.key()).asString();
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
     * @return Head info on this content. Fails if head is not a singleton.
     */
    public ContentInfo getInfo() {
        return tree.get(getOnlyElement(tree.getHead()));
    }

    /**
     * @return A tree containing the info revision of this content.
     */
    public ContentInfoTree getTree() {
        return tree;
    }
}
