package store.common;

import static com.google.common.base.Objects.toStringHelper;
import static java.util.Collections.unmodifiableSortedSet;
import java.util.Map;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import java.util.SortedSet;
import java.util.TreeSet;
import static store.common.MappableUtil.fromList;
import static store.common.MappableUtil.toList;
import store.common.hash.Hash;
import store.common.value.Value;

/**
 * Actual result of a command.
 */
public final class CommandResult implements Mappable {

    private static final String TRANSACTION_ID = "transactionId";
    private static final String OPERATION = "operation";
    private static final String NO_OP = "noOp";
    private static final String HEAD = "head";
    private long transactionId;
    private final Operation operation;
    private final SortedSet<Hash> head;

    private CommandResult(long transactionId, Operation operation, SortedSet<Hash> head) {
        this.transactionId = transactionId;
        this.operation = operation;
        this.head = unmodifiableSortedSet(new TreeSet<>(head));
    }

    /**
     * Static factory method.
     *
     * @param transactionId Associated transaction identifier.
     * @param operation Operation executed.
     * @param head New head after command execution.
     * @return A new instance.
     */
    public static CommandResult of(long transactionId, Operation operation, SortedSet<Hash> head) {
        return new CommandResult(transactionId, requireNonNull(operation), head);
    }

    /**
     * Specific static factory method for no-op command result.
     *
     * @param transactionId Associated transaction identifier.
     * @param head Head after (and before) command execution.
     * @return A new instance.
     */
    public static CommandResult noOp(long transactionId, SortedSet<Hash> head) {
        return new CommandResult(transactionId, null, head);
    }

    /**
     * Provides this command associated transaction identifier.
     *
     * @return A transaction identifier.
     */
    public long getTransactionId() {
        return transactionId;
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
     * Provides head after command execution.
     *
     * @return A sorted set of revisions.
     */
    public SortedSet<Hash> getHead() {
        return head;
    }

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(TRANSACTION_ID, transactionId)
                .put(OPERATION, isNoOp() ? NO_OP : operation.toString())
                .put(HEAD, toList(head))
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static CommandResult fromMap(Map<String, Value> map) {
        long transactionId = map.get(TRANSACTION_ID).asLong();
        SortedSet<Hash> head = fromList(map.get(HEAD).asList());
        String opCode = map.get(OPERATION).asString();
        if (opCode.equals(NO_OP)) {
            return CommandResult.noOp(transactionId, head);
        } else {
            return CommandResult.of(transactionId, Operation.fromString(opCode), head);
        }
    }

    @Override
    public int hashCode() {
        return hash(operation, head);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CommandResult)) {
            return false;
        }
        CommandResult other = (CommandResult) obj;
        return new EqualsBuilder()
                .append(transactionId, other.transactionId)
                .append(operation, other.operation)
                .append(head, other.head)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(TRANSACTION_ID, transactionId)
                .add(OPERATION, operation == null ? NO_OP : operation)
                .add(HEAD, head)
                .toString();
    }
}
