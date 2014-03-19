package store.server.volume;

import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree;
import store.common.ContentInfoTree.ContentInfoTreeBuilder;
import store.common.Hash;
import store.common.Operation;
import store.common.io.ObjectDecoder;
import store.common.io.ObjectEncoder;
import store.common.value.Value;
import store.server.CommandResult;
import store.server.RevSpec;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.RevSpecCheckingFailedException;
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

    public CommandResult put(ContentInfo info, RevSpec revSpec) {
        Optional<ContentInfoTree> existing = load(info.getHash(), revSpec);
        ContentInfoTree updated;
        if (!existing.isPresent()) {
            updated = new ContentInfoTreeBuilder()
                    .add(info)
                    .build();
        } else {
            updated = existing.get()
                    .add(info)
                    .merge();
        }
        save(updated);
        return result(existing, updated);
    }

    public CommandResult put(ContentInfoTree tree, RevSpec revSpec) {
        Optional<ContentInfoTree> existing = load(tree.getHash(), revSpec);
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

    public CommandResult delete(Hash hash, RevSpec revSpec) {
        Optional<ContentInfoTree> existing = load(hash, revSpec);
        if (!existing.isPresent()) {
            throw new UnknownContentException();
        }
        ContentInfoTree updated;
        if (existing.get().isDeleted()) {
            updated = existing.get();
        } else {
            updated = existing.get().add(new ContentInfoBuilder()
                    .withHash(hash)
                    .withLength(existing.get().getLength())
                    .withParents(existing.get().getHead())
                    .withDeleted(true)
                    .computeRevAndBuild());
        }
        save(updated);
        return result(existing, updated);
    }

    private Optional<ContentInfoTree> load(Hash hash, RevSpec revSpec) {
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
        if (!isMatching(existing, revSpec)) {
            throw new RevSpecCheckingFailedException();
        }
        return existing;
    }

    private static boolean isMatching(Optional<ContentInfoTree> tree, RevSpec revSpec) {
        if (!tree.isPresent()) {
            return revSpec.isNone();
        }
        return revSpec.matches(tree.get().getHead());
    }

    private void save(ContentInfoTree tree) {
        if (!tree.getUnknownParents().isEmpty()) {
            throw new UnknownRevisionException();
        }
        Path path = path(tree.getHash());
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
                .resolve(hash.encode());
    }

    private static CommandResult result(Optional<ContentInfoTree> before, ContentInfoTree after) {
        Optional<Operation> operation = operation(before, after);
        if (!operation.isPresent()) {
            return CommandResult.noOp(after.getHead());
        }
        return CommandResult.of(operation.get(), after.getHead());
    }

    private static Optional<Operation> operation(Optional<ContentInfoTree> before, ContentInfoTree after) {
        if (!before.isPresent()) {
            return Optional.of(Operation.CREATE);
        }
        if (before.get().equals(after)) {
            return Optional.absent();
        }
        boolean beforeIsDeleted = before.get().isDeleted();
        boolean afterIsDeleted = after.isDeleted();

        if (beforeIsDeleted && !afterIsDeleted) {
            return Optional.of(Operation.RESTORE);
        }

        if (!beforeIsDeleted && afterIsDeleted) {
            return Optional.of(Operation.DELETE);
        }
        return Optional.of(Operation.UPDATE);
    }

    public Optional<ContentInfo> get(Hash hash) {
        // TODO This is a stub
        // Devrait prendre en compte la RevSpec
        // Revoir l'API pour pouvoir retourner un arbre au besoin !

        Optional<ContentInfoTree> tree = load(hash, RevSpec.any());
        if (tree.isPresent()) {
            return Optional.of(tree.get().get(tree.get().getHead().first()));
        }
        return Optional.absent();
    }

    private static void save(ContentInfoTree tree, Output output) {
        for (ContentInfo info : tree.list()) {
            ObjectEncoder encoder = new ObjectEncoder()
                    .put("hash", info.getHash().getBytes())
                    .put("length", info.getLength())
                    .put("rev", info.getRev().getBytes());

            List<Value> list = new ArrayList<>();
            for (Hash parent : info.getParents()) {
                list.add(Value.of(parent.getBytes()));
            }
            encoder.put("parents", list);
            if (info.isDeleted()) {
                encoder.put("deleted", true);
            }
            byte[] bytes = encoder
                    .put("metadata", info.getMetadata())
                    .build();

            output.write(bytes);
        }
    }

    private static ContentInfoTree load(Input input) {
        ContentInfoTreeBuilder treeBuilder = new ContentInfoTreeBuilder();
        try (StreamDecoder streamDecoder = new StreamDecoder(input)) {
            while (streamDecoder.hasNext()) {
                ObjectDecoder objectDecoder = streamDecoder.next();
                ContentInfoBuilder builder = new ContentInfoBuilder();
                for (Object obj : objectDecoder.getList("parents")) {
                    builder.withParent(new Hash((byte[]) obj));
                }
                if (objectDecoder.containsKey("deleted")) {
                    builder.withDeleted(objectDecoder.getBoolean("deleted"));
                }
                treeBuilder.add(builder
                        .withHash(new Hash(objectDecoder.getByteArray("hash")))
                        .withLength(objectDecoder.getLong("length"))
                        .withMetadata(objectDecoder.getMap("metadata"))
                        .build(new Hash(objectDecoder.getByteArray("rev"))));
            }
        }
        return treeBuilder.build();
    }
}
