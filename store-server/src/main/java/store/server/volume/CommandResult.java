package store.server.volume;

import static com.google.common.base.Objects.toStringHelper;
import static java.util.Collections.unmodifiableSortedSet;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import java.util.SortedSet;
import java.util.TreeSet;
import store.common.Hash;
import store.common.Operation;

/**
 * Actual result of a command.
 */
public final class CommandResult {

    private final Operation operation;
    private final SortedSet<Hash> head;

    private CommandResult(Operation operation, SortedSet<Hash> head) {
        this.operation = operation;
        this.head = unmodifiableSortedSet(new TreeSet<>(head));
    }

    /**
     * Static factory method.
     *
     * @param operation Operation executed.
     * @param head New head after command execution.
     * @return A new instance.
     */
    public static CommandResult of(Operation operation, SortedSet<Hash> head) {
        return new CommandResult(requireNonNull(operation), head);
    }

    /**
     * Specific static factory method for no-op command result.
     *
     * @param head Head after (and before) command execution.
     * @return A new instance.
     */
    public static CommandResult noOp(SortedSet<Hash> head) {
        return new CommandResult(null, head);
    }

    /**
     * Checks if command actually lead to a concrete operation.
     *
     * @return true if no operation actually took place
     */
    public boolean isNoOp() {
        return operation == null;
    }

    /**
     * Provides operation actually executed. Fails if this is a no-op.
     *
     * @return An operation
     */
    public Operation getOperation() {
        if (isNoOp()) {
            throw new IllegalStateException();
        }
        return operation;
    }

    /**
     * Provides head after command execution.
     *
     * @return A sorted set of revisions.
     */
    public SortedSet<Hash> getHead() {
        return head;
    }

    @Override
    public int hashCode() {
        return hash(operation, head);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CommandResult) {
            CommandResult other = (CommandResult) obj;
            return operation == other.operation && head.equals(other.head);
        }
        return false;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("operation", operation == null ? "noOp" : operation)
                .add("head", head)
                .toString();
    }
}
