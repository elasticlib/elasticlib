package store.server.repository;

import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.SortedSet;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Operation;
import store.common.hash.Hash;
import store.server.exception.ConflictException;
import store.server.exception.UnknownContentException;
import store.server.exception.UnknownRevisionException;
import static store.server.manager.storage.DatabaseEntries.asMappable;
import static store.server.manager.storage.DatabaseEntries.entry;
import store.server.manager.storage.StorageManager;

/**
 * Stores and retrieves content info inside a repository.
 */
class InfoManager {

    private static final String INFO = "info";
    private final StorageManager storageManager;
    private final Database database;

    public InfoManager(StorageManager storageManager) {
        this.storageManager = storageManager;
        this.database = storageManager.openDatabase(INFO);
    }

    public CommandResult put(ContentInfo info) {
        Optional<ContentInfoTree> existing = load(info.getContent(), LockMode.RMW);
        ContentInfoTree updated;
        if (!existing.isPresent()) {
            if (!info.getParents().isEmpty()) {
                throw new ConflictException();
            }
            updated = new ContentInfoTree.ContentInfoTreeBuilder()
                    .add(info)
                    .build();
        } else {
            if (!existing.get().getHead().equals(info.getParents())) {
                throw new ConflictException();
            }
            updated = existing.get()
                    .add(info)
                    .merge();
        }
        save(updated);
        return result(existing, updated);
    }

    public CommandResult put(ContentInfoTree tree) {
        Optional<ContentInfoTree> existing = load(tree.getContent(), LockMode.RMW);
        ContentInfoTree updated;
        if (!existing.isPresent()) {
            updated = tree;
        } else {
            updated = existing.get()
                    .add(tree)
                    .merge();
        }
        save(updated);
        return result(existing, updated);
    }

    public CommandResult delete(Hash hash, SortedSet<Hash> head) {
        Optional<ContentInfoTree> existing = load(hash, LockMode.RMW);
        if (!existing.isPresent()) {
            throw new UnknownContentException();
        }
        if (!existing.get().getHead().equals(head)) {
            throw new ConflictException();
        }
        ContentInfoTree updated;
        if (existing.get().isDeleted()) {
            updated = existing.get();
        } else {
            updated = existing.get().add(new ContentInfo.ContentInfoBuilder()
                    .withContent(hash)
                    .withLength(existing.get().getLength())
                    .withParents(existing.get().getHead())
                    .withDeleted(true)
                    .computeRevisionAndBuild());
        }
        save(updated);
        return result(existing, updated);
    }

    public Optional<ContentInfoTree> get(Hash hash) {
        return load(hash, LockMode.DEFAULT);
    }

    private Optional<ContentInfoTree> load(Hash hash, LockMode lockMode) {
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status = database.get(storageManager.currentTransaction(),
                                              entry(hash),
                                              data,
                                              lockMode);

        if (status == OperationStatus.NOTFOUND) {
            return Optional.absent();
        }
        return Optional.of(asMappable(data, ContentInfoTree.class));
    }

    private void save(ContentInfoTree tree) {
        if (!tree.getUnknownParents().isEmpty()) {
            throw new UnknownRevisionException();
        }
        database.put(storageManager.currentTransaction(), entry(tree.getContent()), entry(tree));
    }

    private CommandResult result(Optional<ContentInfoTree> before, ContentInfoTree after) {
        long id = storageManager.currentTransaction().getId();
        Optional<Operation> operation = operation(before, after);
        if (!operation.isPresent()) {
            return CommandResult.noOp(id, after.getContent(), after.getHead());
        }
        return CommandResult.of(id, operation.get(), after.getContent(), after.getHead());
    }

    private static Optional<Operation> operation(Optional<ContentInfoTree> before, ContentInfoTree after) {
        boolean beforeIsDeleted = !before.isPresent() || before.get().isDeleted();
        boolean afterIsDeleted = after.isDeleted();

        if (before.isPresent() && before.get().equals(after)) {
            return Optional.absent();
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
