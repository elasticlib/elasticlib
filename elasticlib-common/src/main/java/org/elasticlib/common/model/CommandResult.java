package org.elasticlib.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import java.util.SortedSet;
import java.util.TreeSet;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import static org.elasticlib.common.mappable.MappableUtil.putRevisions;
import static org.elasticlib.common.mappable.MappableUtil.revisions;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

/**
 * Actual result of a command.
 */
public final class CommandResult implements Mappable {

    private static final String OPERATION = "operation";
    private static final String NO_OP = "noOp";
    private static final String CONTENT = "content";
    private static final String REVISIONS = "revisions";
    private final Operation operation;
    private final Hash content;
    private final SortedSet<Hash> revisions;

    private CommandResult(Operation operation, Hash content, SortedSet<Hash> revisions) {
        this.operation = operation;
        this.content = content;
        this.revisions = unmodifiableSortedSet(new TreeSet<>(revisions));
    }

    /**
     * Static factory method.
     *
     * @param operation Operation executed.
     * @param content Associated content hash.
     * @param revisions New head revisions after command execution.
     * @return A new instance.
     */
    public static CommandResult of(Operation operation, Hash content, SortedSet<Hash> revisions) {
        return new CommandResult(requireNonNull(operation), content, revisions);
    }

    /**
     * Specific static factory method for no-op command result.
     *
     * @param content Associated content hash.
     * @param revisions Head revisions after (and before) command execution.
     * @return A new instance.
     */
    public static CommandResult noOp(Hash content, SortedSet<Hash> revisions) {
        return new CommandResult(null, content, revisions);
    }

    /**
     * Checks if command actually lead to a concrete operation.
     *
     * @return true if no operation actually took place.
     */
    public boolean isNoOp() {
        return operation == null;
    }

    /**
     * Provides operation actually executed. Fails if this is a no-op.
     *
     * @return An operation.
     */
    public Operation getOperation() {
        if (isNoOp()) {
            throw new IllegalStateException();
        }
        return operation;
    }

    /**
     * Provides this command associated content hash.
     *
     * @return A hash.
     */
    public Hash getContent() {
        return content;
    }

    /**
     * Provides revision hashes after command execution.
     *
     * @return A sorted set of revision hashes.
     */
    public SortedSet<Hash> getRevisions() {
        return revisions;
    }

    @Override
    public Map<String, Value> toMap() {
        MapBuilder builder = new MapBuilder()
                .put(OPERATION, isNoOp() ? NO_OP : operation.toString())
                .put(CONTENT, content);

        return putRevisions(builder, revisions).build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static CommandResult fromMap(Map<String, Value> map) {
        Hash content = map.get(CONTENT).asHash();
        SortedSet<Hash> revisions = revisions(map);
        String opCode = map.get(OPERATION).asString();
        if (opCode.equals(NO_OP)) {
            return CommandResult.noOp(content, revisions);
        } else {
            return CommandResult.of(Operation.fromString(opCode), content, revisions);
        }
    }

    @Override
    public int hashCode() {
        return hash(operation, content, revisions);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CommandResult)) {
            return false;
        }
        CommandResult other = (CommandResult) obj;
        return new EqualsBuilder()
                .append(operation, other.operation)
                .append(content, other.content)
                .append(revisions, other.revisions)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(OPERATION, operation == null ? NO_OP : operation)
                .add(CONTENT, content)
                .add(REVISIONS, revisions)
                .toString();
    }
}
