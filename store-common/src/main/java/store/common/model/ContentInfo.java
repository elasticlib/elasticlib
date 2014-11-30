package store.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import static java.util.stream.Collectors.toList;
import store.common.hash.Hash;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import store.common.util.EqualsBuilder;
import store.common.value.Value;

/**
 * Holds info about a content.
 */
public final class ContentInfo implements Mappable {

    private static final String STAGING = "staging";
    private static final String STATE = "state";
    private static final String HASH = "hash";
    private static final String LENGTH = "length";
    private static final String HEAD = "head";
    private final ContentState state;
    private final Hash hash;
    private final long length;
    private final List<Revision> head;

    /**
     * Constructor.
     *
     * @param state Content state.
     * @param hash (Staged) content hash.
     * @param length (Staged) content length.
     * @param head Content head revisions.
     */
    public ContentInfo(ContentState state, Hash hash, long length, List<Revision> head) {
        this.state = state;
        this.hash = hash;
        this.length = length;
        this.head = unmodifiableList(head);
    }

    /**
     * Provides the content state.
     *
     * @return The content state.
     */
    public ContentState getState() {
        return state;
    }

    /**
     * Provides the staged content hash.
     *
     * @return The content hash.
     */
    public Hash getHash() {
        return hash;
    }

    /**
     * Provides the staged content length.
     *
     * @return The content length.
     */
    public long getLength() {
        return length;
    }

    /**
     * Provides head revisions of the content. Returned list is empty if content has never been present.
     *
     * @return Head revisions of the content.
     */
    public List<Revision> getHead() {
        return head;
    }

    @Override
    public Map<String, Value> toMap() {
        List<Value> values = head.stream()
                .map(rev -> Value.of(rev.toMap()))
                .collect(toList());

        Map<String, Value> staging = new MapBuilder()
                .put(HASH, hash)
                .put(LENGTH, length)
                .build();

        return new MapBuilder()
                .put(STATE, state.toString())
                .put(STAGING, staging)
                .put(HEAD, values).build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static ContentInfo fromMap(Map<String, Value> map) {
        Map<String, Value> staging = map.get(STAGING).asMap();
        List<Revision> head = map.get(HEAD)
                .asList()
                .stream()
                .map(value -> Revision.fromMap(value.asMap()))
                .collect(toList());

        return new ContentInfo(ContentState.fromString(map.get(STATE).asString()),
                               staging.get(HASH).asHash(),
                               staging.get(LENGTH).asLong(),
                               head);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state,
                            hash,
                            length,
                            head);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ContentInfo)) {
            return false;
        }
        ContentInfo other = (ContentInfo) obj;
        return new EqualsBuilder()
                .append(state, other.state)
                .append(hash, other.hash)
                .append(length, other.length)
                .append(head, other.head)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(STATE, state)
                .add(HASH, hash)
                .add(LENGTH, length)
                .add(HEAD, head)
                .toString();
    }
}
