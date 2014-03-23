package store.server.service;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.Collection;
import static java.util.Collections.unmodifiableSortedSet;
import static java.util.Objects.hash;
import java.util.SortedSet;
import java.util.TreeSet;
import store.common.EqualsBuilder;
import store.common.Hash;

/**
 * Specify an expected set of revisions to apply a request on.
 * <p>
 * A spec may be:<br>
 * - Any, which actually makes no expectation.<br>
 * - None, which expects no revisions at all.<br>
 * - An explicit set of expected revision hashes.
 */
public final class RevSpec {

    /**
     * Constant for 'none' spec.
     */
    public static final String NONE = "none";
    /**
     * Constant for 'any' spec.
     */
    public static final String ANY = "any";
    private static final RevSpec NONE_SPEC = new RevSpec(false);
    private static final RevSpec ANY_SPEC = new RevSpec(true);
    private final SortedSet<Hash> revs;
    private final boolean any;

    private RevSpec(SortedSet<Hash> revs) {
        this.revs = unmodifiableSortedSet(revs);
        this.any = false;
    }

    private RevSpec(boolean any) {
        this.revs = new TreeSet<>();
        this.any = any;
    }

    /**
     * Parses supplied string argument and provides corresponding RevSpec.
     * <p>
     * Supplied string may be:<br>
     * - 'any' for any spec.<br>
     * - 'none' for none spec.<br>
     * - A dash-separated sequence of encoded hashes.
     *
     * @param arg String argument to parse.
     * @return Parsed RevSpec.
     */
    public static RevSpec valueOf(String arg) {
        switch (arg) {
            case NONE:
                return NONE_SPEC;
            case ANY:
                return ANY_SPEC;
            default:
                return new RevSpec(parse(arg));
        }
    }

    private static SortedSet<Hash> parse(String arg) {
        SortedSet<Hash> revs = new TreeSet<>();
        for (String part : Splitter.on('-').split(arg)) {
            revs.add(new Hash(part));
        }
        return revs;
    }

    /**
     * @param revs A collection of revision hashes.
     * @return Corresponding spec.
     */
    public static RevSpec revs(Collection<Hash> revs) {
        return new RevSpec(new TreeSet<>(revs));
    }

    /**
     * @return The none spec.
     */
    public static RevSpec none() {
        return NONE_SPEC;
    }

    /**
     * @return The any spec.
     */
    public static RevSpec any() {
        return ANY_SPEC;
    }

    /**
     * Returns the set of specified revisions. Fails if this spec is the any one.
     *
     * @return An immutable set of hashes.
     */
    public SortedSet<Hash> getRevs() {
        if (isAny()) {
            throw new IllegalStateException();
        }
        return revs;
    }

    /**
     * Checks if supplied head matches this spec.
     *
     * @param head A set of hashes.
     * @return true if this the any spec or if supplied head equals this revs set.
     */
    public boolean matches(SortedSet<Hash> head) {
        return isAny() || revs.equals(head);
    }

    /**
     * @return true if this is the none spec.
     */
    public boolean isNone() {
        return revs.isEmpty();
    }

    /**
     * @return true if this is the any spec.
     */
    public boolean isAny() {
        return any;
    }

    @Override
    public int hashCode() {
        return hash(revs, any);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RevSpec)) {
            return false;
        }
        RevSpec other = (RevSpec) obj;
        return new EqualsBuilder()
                .append(revs, other.revs)
                .append(any, other.any)
                .build();
    }

    @Override
    public String toString() {
        if (isAny()) {
            return ANY;
        }
        if (isNone()) {
            return NONE;
        }
        return Joiner.on('-').join(revs);
    }
}
