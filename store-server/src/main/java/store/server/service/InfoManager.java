package store.server.service;

import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SortedSet;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree;
import store.common.ContentInfoTree.ContentInfoTreeBuilder;
import store.common.Operation;
import store.common.bson.BsonWriter;
import store.common.hash.Hash;
import store.server.exception.ConflictException;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.UnknownContentException;
import store.server.exception.UnknownRevisionException;
import store.server.exception.WriteException;
import store.server.transaction.Input;
import store.server.transaction.Output;
import store.server.transaction.TransactionContext;

class InfoManager {

    private static final int KEY_LENGTH = 2;
    private final Path root;

    private InfoManager(final Path root) {
        this.root = root;
    }

    public static InfoManager create(Path path) {
        try {
            Files.createDirectory(path);
            for (String key : Hash.keySet(KEY_LENGTH)) {
                Files.createDirectory(path.resolve(key));
            }
            return new InfoManager(path);

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    public static InfoManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        return new InfoManager(path);
    }

    public CommandResult put(ContentInfo info) {
        Optional<ContentInfoTree> existing = get(info.getContent());
        ContentInfoTree updated;
        if (!existing.isPresent()) {
            if (!info.getParents().isEmpty()) {
                throw new ConflictException();
            }
            updated = new ContentInfoTreeBuilder()
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
        Optional<ContentInfoTree> existing = get(tree.getContent());
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
        Optional<ContentInfoTree> existing = get(hash);
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
            updated = existing.get().add(new ContentInfoBuilder()
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
        Path path = path(hash);
        TransactionContext txContext = TransactionContext.current();
        Optional<ContentInfoTree> existing;
        if (!txContext.exists(path)) {
            existing = Optional.absent();
        } else {
            try (Input input = txContext.openInput(path)) {
                existing = Optional.of(load(input));
            }
        }
        return existing;
    }

    private void save(ContentInfoTree tree) {
        if (!tree.getUnknownParents().isEmpty()) {
            throw new UnknownRevisionException();
        }
        Path path = path(tree.getContent());
        TransactionContext txContext = TransactionContext.current();
        if (!txContext.exists(path)) {
            txContext.create(path);
        }
        try (Output output = txContext.openOutput(path)) {
            save(tree, output);
        }
    }

    private Path path(Hash hash) {
        return root
                .resolve(hash.key(KEY_LENGTH))
                .resolve(hash.asHexadecimalString());
    }

    private static CommandResult result(Optional<ContentInfoTree> before, ContentInfoTree after) {
        long id = TransactionContext.current().getId();
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

    private static void save(ContentInfoTree tree, Output output) {
        for (ContentInfo info : tree.list()) {
            byte[] bytes = new BsonWriter()
                    .put(info.toMap())
                    .build();

            output.write(bytes);
        }
    }

    private static ContentInfoTree load(Input input) {
        ContentInfoTreeBuilder treeBuilder = new ContentInfoTreeBuilder();
        try (BsonStreamReader streamReader = new BsonStreamReader(input)) {
            while (streamReader.hasNext()) {
                ContentInfo contentInfo = ContentInfo.fromMap(streamReader.next().asMap());
                treeBuilder.add(contentInfo);
            }
        }
        return treeBuilder.build();
    }
}
