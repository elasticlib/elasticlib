package store.server;

import static com.google.common.collect.Iterables.getOnlyElement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.Thread.currentThread;
import java.util.Arrays;
import static java.util.Arrays.copyOfRange;
import java.util.SortedSet;
import store.common.hash.Digest;
import store.common.hash.Hash;
import static store.common.metadata.Properties.Common.CONTENT_TYPE;
import static store.common.metadata.Properties.Common.FILE_NAME;
import store.common.model.Revision;
import store.common.model.Revision.RevisionBuilder;
import store.common.model.RevisionTree;
import store.common.model.RevisionTree.RevisionTreeBuilder;
import static store.common.util.IoUtil.copyAndDigest;
import store.common.value.Value;

/**
 * Represents a test content, with its metadata.
 */
public final class TestContent {

    private final byte[] bytes;
    private final RevisionTree tree;

    private TestContent(byte[] bytes, RevisionTree tree) {
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

            Revision revision = new RevisionBuilder()
                    .withLength(digest.getLength())
                    .withContent(digest.getHash())
                    .with(FILE_NAME.key(), Value.of(filename))
                    .with(CONTENT_TYPE.key(), Value.of(contentType))
                    .computeRevisionAndBuild();

            return new TestContent(outputStream.toByteArray(),
                                   new RevisionTreeBuilder().add(revision).build());

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
        Revision head = getRevision();
        return new TestContent(bytes, tree.add(new RevisionBuilder()
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
        Revision head = getRevision();
        return new TestContent(bytes, tree.add(new RevisionBuilder()
                               .withParent(head.getRevision())
                               .withLength(head.getLength())
                               .withContent(head.getContent())
                               .withDeleted(true)
                               .computeRevisionAndBuild()));
    }

    /**
     * Provides an input-stream on this content.
     *
     * @return An input-stream.
     */
    public InputStream getInputStream() {
        return new ByteArrayInputStream(bytes);
    }

    /**
     * Provides an input-stream on a range of this content.
     *
     * @return An input-stream.
     * @param offset The position of first byte to return, inclusive. Expected to be positive or zero.
     * @param length The amount of bytes to returns. Expected to be positive or zero.
     */
    public InputStream getInputStream(long offset, long length) {
        return new ByteArrayInputStream(copyOfRange(bytes, (int) offset, (int) (offset + length)));
    }

    /**
     * @return This content as a byte array.
     */
    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * @return This content's filename.
     */
    public String filename() {
        return getRevision().getMetadata().get(FILE_NAME.key()).asString();
    }

    /**
     * @return This content's type.
     */
    public String contentType() {
        return getRevision().getMetadata().get(CONTENT_TYPE.key()).asString();
    }

    /**
     * @return This content's hash.
     */
    public Hash getHash() {
        return tree.getContent();
    }

    /**
     * @return This content's length.
     */
    public long getLength() {
        return tree.getLength();
    }

    /**
     * @return Head of a the tree associated with this content.
     */
    public SortedSet<Hash> getHead() {
        return tree.getHead();
    }

    /**
     * @return The single head revision on this content. Fails if head is not a singleton.
     */
    public Revision getRevision() {
        return tree.get(getOnlyElement(tree.getHead()));
    }

    /**
     * @return A tree containing the info revision of this content.
     */
    public RevisionTree getTree() {
        return tree;
    }
}
