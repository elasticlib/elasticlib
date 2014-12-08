package org.elasticlib.node.repository;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.Optional;
import java.util.SortedSet;
import org.elasticlib.common.exception.ConflictException;
import org.elasticlib.common.exception.UnknownContentException;
import org.elasticlib.common.exception.UnknownRevisionException;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.Operation;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.Revision.RevisionBuilder;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.RevisionTree.RevisionTreeBuilder;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asMappable;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Stores and retrieves revisions inside a repository.
 */
class RevisionManager {

    private static final String REVISION = "revision";

    private final StorageManager storageManager;
    private final Database database;

    /**
     * Constructor.
     *
     * @param storageManager Underlying storage manager.
     */
    public RevisionManager(StorageManager storageManager) {
        this.storageManager = storageManager;
        this.database = storageManager.openDatabase(REVISION);
    }

    /**
     * Adds a new revision.
     *
     * @param revision Revision to add.
     * @return Actual result.
     */
    public CommandResult put(Revision revision) {
        Optional<RevisionTree> existing = load(revision.getContent(), LockMode.RMW);
        RevisionTree updated;
        if (!existing.isPresent()) {
            if (!revision.getParents().isEmpty()) {
                throw new ConflictException();
            }
            updated = new RevisionTreeBuilder()
                    .add(revision)
                    .build();
        } else {
            if (existing.get().contains(revision.getRevision())) {
                updated = existing.get();

            } else if (!existing.get().getHead().equals(revision.getParents())) {
                throw new ConflictException();

            } else {
                updated = existing.get()
                        .add(revision)
                        .merge();
            }
        }
        return save(existing, updated);
    }

    /**
     * Adds a new revision tree.
     *
     * @param info Revision tree to add.
     * @return Actual result.
     */
    public CommandResult put(RevisionTree tree) {
        Optional<RevisionTree> existing = load(tree.getContent(), LockMode.RMW);
        RevisionTree updated;
        if (!existing.isPresent()) {
            updated = tree;
        } else {
            updated = existing.get()
                    .add(tree)
                    .merge();
        }
        return save(existing, updated);
    }

    /**
     * Marks an existing content as deleted.
     *
     * @param hash Content hash.
     * @param head Expected associated head, for optimistic concurrency purpose.
     * @return Actual result.
     */
    public CommandResult delete(Hash hash, SortedSet<Hash> head) {
        Optional<RevisionTree> existing = load(hash, LockMode.RMW);
        if (!existing.isPresent()) {
            throw new UnknownContentException();
        }
        if (!existing.get().getHead().equals(head)) {
            throw new ConflictException();
        }
        RevisionTree updated;
        if (existing.get().isDeleted()) {
            updated = existing.get();
        } else {
            updated = existing.get().add(new RevisionBuilder()
                    .withContent(hash)
                    .withLength(existing.get().getLength())
                    .withParents(existing.get().getHead())
                    .withDeleted(true)
                    .computeRevisionAndBuild());
        }
        return save(existing, updated);
    }

    /**
     * Loads revision tree of a content.
     *
     * @param hash Content hash.
     * @return Associated revision tree, if any.
     */
    public Optional<RevisionTree> get(Hash hash) {
        return load(hash, LockMode.DEFAULT);
    }

    private Optional<RevisionTree> load(Hash hash, LockMode lockMode) {
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status = database.get(storageManager.currentTransaction(),
                                              entry(hash),
                                              data,
                                              lockMode);

        if (status == OperationStatus.NOTFOUND) {
            return Optional.empty();
        }
        return Optional.of(asMappable(data, RevisionTree.class));
    }

    private CommandResult save(Optional<RevisionTree> before, RevisionTree after) {
        Optional<Operation> operation = operation(before, after);
        if (!operation.isPresent()) {
            return CommandResult.noOp(after.getContent(), after.getHead());
        }
        if (!after.getUnknownParents().isEmpty()) {
            throw new UnknownRevisionException();
        }
        database.put(storageManager.currentTransaction(), entry(after.getContent()), entry(after));
        return CommandResult.of(operation.get(), after.getContent(), after.getHead());
    }

    private static Optional<Operation> operation(Optional<RevisionTree> before, RevisionTree after) {
        boolean beforeIsDeleted = !before.isPresent() || before.get().isDeleted();
        boolean afterIsDeleted = after.isDeleted();

        if (before.isPresent() && before.get().equals(after)) {
            return Optional.empty();
        }
        if (beforeIsDeleted && !afterIsDeleted) {
            return Optional.of(Operation.CREATE);
        }
        if (!beforeIsDeleted && afterIsDeleted) {
            return Optional.of(Operation.DELETE);
        }
        return Optional.of(Operation.UPDATE);
    }
}
