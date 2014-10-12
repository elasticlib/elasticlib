package store.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import java.util.ArrayList;
import java.util.Collections;
import static java.util.Collections.unmodifiableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import store.common.hash.Hash;
import store.common.mappable.MapBuilder;
import store.common.mappable.Mappable;
import store.common.util.EqualsBuilder;
import store.common.value.Value;

/**
 * Holds info about a content.
 */
public final class ContentInfo implements Mappable {

    private static final String CONTENT = "content";
    private static final String STATE = "state";
    private static final String HASH = "hash";
    private static final String LENGTH = "length";
    private static final String HEAD = "head";
    private static final ContentInfo ABSENT_INFO = new ContentInfo(ContentState.ABSENT,
                                                                   null,
                                                                   0,
                                                                   Collections.<Revision>emptyList());
    private final ContentState state;
    private final Hash hash;
    private final long length;
    private final List<Revision> head;

    private ContentInfo(ContentState state, Hash hash, long length, List<Revision> head) {
        this.state = state;
        this.hash = hash;
        this.length = length;
        this.head = head;
    }

    /**
     * Provides info of an absent content.
     *
     * @return A ContentInfo instance.
     */
    public static ContentInfo absent() {
        return ABSENT_INFO;
    }

    /**
     * Provides info of a (partially) staged content.
     *
     * @param state Content state. Expected to be PARTIAL, STAGING or STAGED.
     * @param hash Content hash.
     * @param length Content length.
     * @return A ContentInfo instance.
     */
    public static ContentInfo of(ContentState state, Hash hash, long length) {
        checkArgument(isMatching(state, ContentState.PARTIAL, ContentState.STAGING, ContentState.STAGED), state.name());
        return new ContentInfo(state, hash, length, Collections.<Revision>emptyList());
    }

    /**
     * Provides info of a present/deleted content.
     *
     * @param head Content head revisions. Expected not to be empty.
     * @return A ContentInfo instance.
     */
    public static ContentInfo of(List<Revision> head) {
        checkArgument(!head.isEmpty(), "head is empty");
        return new ContentInfo(isPresent(head) ? ContentState.PRESENT : ContentState.DELETED,
                               head.get(0).getContent(),
                               head.get(0).getLength(),
                               unmodifiableList(head));
    }

    private static boolean isPresent(List<Revision> head) {
        for (Revision rev : head) {
            if (!rev.isDeleted()) {
                return true;
            }
        }
        return false;
    }

    private static boolean isMatching(ContentState actual, ContentState... expected) {
        for (ContentState state : expected) {
            if (actual == state) {
                return true;
            }
        }
        return false;
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
     * Provides the content hash. If content is partially staged, hash of the staged part is returned. Fails if content
     * is absent.
     *
     * @return The content hash.
     */
    public Hash getHash() {
        checkState(state != ContentState.ABSENT);
        return hash;
    }

    /**
     * Provides the content length. If content is partially staged, length of the staged part is returned. Fails if
     * content is absent.
     *
     * @return The content length.
     */
    public long getLength() {
        checkState(state != ContentState.ABSENT);
        return length;
    }

    /**
     * Provides head revisions of the content. Fails if content is neither present, nor deleted.
     *
     * @return Head revisions of the content.
     */
    public List<Revision> getHead() {
        checkState(isMatching(state, ContentState.PRESENT, ContentState.DELETED));
        return head;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .put(STATE, state.toString());

        if (state != ContentState.ABSENT) {
            builder.put(HASH, hash)
                    .put(LENGTH, length);
        }
        if (isMatching(state, ContentState.PRESENT, ContentState.DELETED)) {
            List<Value> values = new ArrayList<>();
            for (Revision rev : head) {
                Map<String, Value> map = rev.toMap();
                map.remove(CONTENT);
                map.remove(LENGTH);
                values.add(Value.of(map));
            }
            builder.put(HEAD, values);
        }
        return builder.build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static ContentInfo fromMap(Map<String, Value> map) {
        ContentState state = ContentState.fromString(map.get(STATE).asString());
        if (state == ContentState.ABSENT) {
            return absent();
        }
        if (isMatching(state, ContentState.PRESENT, ContentState.DELETED)) {
            List<Revision> head = new ArrayList<>();
            for (Value value : map.get(HEAD).asList()) {
                Map<String, Value> revMap = new HashMap<>();
                revMap.put(CONTENT, map.get(HASH));
                revMap.put(LENGTH, map.get(LENGTH));
                revMap.putAll(value.asMap());
                head.add(Revision.fromMap(revMap));
            }
            return of(head);
        }
        return of(state, map.get(HASH).asHash(), map.get(LENGTH).asLong());
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
