/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.repository;

import static com.google.common.base.Joiner.on;
import com.sleepycat.je.Database;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.exception.ContentAlreadyPresentException;
import org.elasticlib.common.exception.IOFailureException;
import org.elasticlib.common.exception.InvalidRepositoryPathException;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.exception.UnknownContentException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.ContentState;
import org.elasticlib.common.model.Digest;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.Operation;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import org.elasticlib.node.manager.message.MessageManager;
import org.elasticlib.node.manager.message.NewRepositoryEvent;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;
import org.elasticlib.node.manager.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local repository implementation. Stores contents with their metadata and performs their asynchronous indexation
 * transparently.
 */
public class LocalRepository implements Repository {

    private static final String STORAGE = "storage";
    private static final String INDEX = "index";
    private static final String STATS = "stats";
    private static final String CUR_SEQS = "curSeqs";
    private static final Logger LOG = LoggerFactory.getLogger(LocalRepository.class);

    private final RepositoryDef def;
    private final StorageManager storageManager;
    private final MessageManager messageManager;
    private final RevisionManager revisionManager;
    private final HistoryManager historyManager;
    private final StatsManager statsManager;
    private final ContentManager contentManager;
    private final Index index;
    private final IndexingAgent indexingAgent;
    private final StatsAgent statsAgent;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private LocalRepository(RepositoryDef def,
                            Config config,
                            TaskManager taskManager,
                            MessageManager messageManager,
                            ContentManager contentManager,
                            Index index) {
        this.def = def;
        storageManager = new StorageManager(def.getName(),
                                            Paths.get(def.getPath()).resolve(STORAGE),
                                            config,
                                            taskManager);
        this.messageManager = messageManager;
        revisionManager = new RevisionManager(storageManager);
        historyManager = new HistoryManager(storageManager);
        statsManager = new StatsManager(storageManager);
        this.contentManager = contentManager;
        this.index = index;

        Database curSeqsDb = storageManager.openDatabase(CUR_SEQS);
        indexingAgent = new IndexingAgent(this, index, curSeqsDb, entry(INDEX));
        statsAgent = new StatsAgent(this, statsManager, curSeqsDb, entry(STATS));

        indexingAgent.start();
        statsAgent.start();
    }

    /**
     * Creates a new repository.
     *
     * @param path Repository home. Expected not to exist.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param messageManager Messaging infrastructure manager.
     * @return Created repository.
     */
    public static LocalRepository create(Path path, Config config, TaskManager taskManager,
                                         MessageManager messageManager) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidRepositoryPathException();
            }
            Files.createDirectory(path.resolve(STORAGE));

        } catch (IOException e) {
            throw new IOFailureException(e);
        }
        AttributesManager attributesManager = AttributesManager.create(path);
        String name = attributesManager.getName();
        Guid guid = attributesManager.getGuid();
        return new LocalRepository(new RepositoryDef(name, guid, path.toString()),
                                   config,
                                   taskManager,
                                   messageManager,
                                   ContentManager.create(name, path, config, taskManager),
                                   Index.create(name, path));
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator().hasNext();
        }
    }

    /**
     * Opens an existing repository.
     *
     * @param path Repository home.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param messageManager Messaging infrastructure manager.
     * @return Opened repository.
     */
    public static LocalRepository open(Path path, Config config, TaskManager taskManager, MessageManager messageManager) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        AttributesManager attributesManager = AttributesManager.open(path);
        String name = attributesManager.getName();
        Guid guid = attributesManager.getGuid();
        return new LocalRepository(new RepositoryDef(name, guid, path.toString()),
                                   config,
                                   taskManager,
                                   messageManager,
                                   ContentManager.open(name, path, config, taskManager),
                                   Index.open(name, path));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        indexingAgent.stop();
        statsAgent.stop();
        index.close();
        storageManager.stop();
        contentManager.close();
    }

    @Override
    public RepositoryDef getDef() {
        log("Returning repository def");
        return def;
    }

    @Override
    public RepositoryInfo getInfo() {
        log("Returning repository info");
        if (closed.get()) {
            return new RepositoryInfo(def);
        }
        return new RepositoryInfo(def, statsManager.stats(), indexingAgent.info(), statsAgent.info());
    }

    @Override
    public StagingInfo stageContent(Hash hash) {
        ensureOpen();
        log("Staging content {}", hash);
        return storageManager.inTransaction(() -> {
            Optional<RevisionTree> treeOpt = revisionManager.get(hash);
            if (treeOpt.isPresent() && !treeOpt.get().isDeleted()) {
                throw new ContentAlreadyPresentException();
            }
            return contentManager.stageContent(hash);
        });
    }

    @Override
    public StagingInfo writeContent(Hash hash, Guid sessionId, InputStream source, long position) {
        ensureOpen();
        log("Writing to staged content {}", hash);
        return contentManager.writeContent(hash, sessionId, source, position);
    }

    @Override
    public void unstageContent(Hash hash, Guid sessionId) {
        ensureOpen();
        log("Ending staging staging session {}", sessionId);
        contentManager.unstageContent(hash, sessionId);
    }

    @Override
    public CommandResult addRevision(Revision revision) {
        ensureOpen();
        log("Adding revision to {}, with head {}", revision.getContent(), revision.getParents());
        CommandResult result = storageManager.inTransaction(() -> {
            CommandResult res = revisionManager.put(revision);
            handleCommandResult(res, revision.getContent());
            return res;
        });
        propagate(result);
        return result;
    }

    @Override
    public CommandResult mergeTree(RevisionTree tree) {
        ensureOpen();
        log("Merging revision tree of {}", tree.getContent());
        CommandResult result = storageManager.inTransaction(() -> {
            CommandResult res = revisionManager.put(tree);
            handleCommandResult(res, tree.getContent());
            return res;
        });
        propagate(result);
        return result;
    }

    @Override
    public CommandResult deleteContent(Hash hash, SortedSet<Hash> head) {
        ensureOpen();
        log("Deleting content {}, with head {}", hash, head);
        CommandResult result = storageManager.inTransaction(() -> {
            CommandResult res = revisionManager.delete(hash, head);
            handleCommandResult(res, hash);
            return res;
        });
        propagate(result);
        return result;
    }

    private void handleCommandResult(CommandResult result, Hash hash) {
        if (result.isNoOp()) {
            return;
        }
        Operation operation = result.getOperation();
        historyManager.add(operation, hash, result.getRevisions());
        if (operation == Operation.CREATE) {
            contentManager.add(hash);
        }
        if (operation == Operation.DELETE) {
            contentManager.delete(hash);
        }
    }

    private void propagate(CommandResult result) {
        if (result.isNoOp()) {
            return;
        }
        indexingAgent.signal();
        statsAgent.signal();
        messageManager.post(new NewRepositoryEvent(def.getGuid()));
    }

    @Override
    public ContentInfo getContentInfo(Hash hash) {
        ensureOpen();
        log("Returning content info of {}", hash);
        return storageManager.inTransaction(() -> {
            List<Revision> head = head(hash);
            StagingInfo stagingInfo = contentManager.getStagingInfo(hash);
            return new ContentInfo(contentState(hash, head, stagingInfo),
                                   stagingInfo.getHash(),
                                   stagingInfo.getLength(),
                                   head);
        });
    }

    private List<Revision> head(Hash hash) {
        Optional<RevisionTree> treeOpt = revisionManager.get(hash);
        if (treeOpt.isPresent()) {
            RevisionTree tree = treeOpt.get();
            return tree.get(tree.getHead());
        }
        return emptyList();
    }

    private static ContentState contentState(Hash hash, List<Revision> head, StagingInfo stagingInfo) {
        if (head.stream().anyMatch(rev -> !rev.isDeleted())) {
            return ContentState.PRESENT;
        }
        if (stagingInfo.getHash().equals(hash)) {
            return ContentState.STAGED;
        }
        if (stagingInfo.getSessionId() != null) {
            return ContentState.STAGING;
        }
        if (stagingInfo.getLength() == 0) {
            return ContentState.ABSENT;
        }
        return ContentState.PARTIAL;
    }

    @Override
    public RevisionTree getTree(Hash hash) {
        ensureOpen();
        log("Returning revision tree of {}", hash);
        return loadRevisionTree(hash);
    }

    @Override
    public List<Revision> getHead(Hash hash) {
        ensureOpen();
        log("Returning head revisions of {}", hash);
        RevisionTree tree = loadRevisionTree(hash);
        return tree.get(tree.getHead());
    }

    @Override
    public List<Revision> getRevisions(Hash hash, Collection<Hash> revs) {
        ensureOpen();
        log("Returning revisions of {} [{}]", hash, on(", ").join(revs));
        return loadRevisionTree(hash).get(revs);
    }

    private RevisionTree loadRevisionTree(Hash hash) {
        return storageManager.inTransaction(() -> {
            Optional<RevisionTree> tree = revisionManager.get(hash);
            if (!tree.isPresent()) {
                throw new UnknownContentException();
            }
            return tree.get();
        });
    }

    @Override
    public InputStream getContent(Hash hash, long offset, long length) {
        ensureOpen();
        log("Returning content {}, offset {}, length {}", hash, offset, length);
        return contentManager.get(hash, offset, length);
    }

    @Override
    public Digest getDigest(Hash hash) {
        ensureOpen();
        log("Returning digest of content {}", hash);
        return contentManager.getDigest(hash);
    }

    @Override
    public Digest getDigest(Hash hash, long offset, long length) {
        ensureOpen();
        log("Returning digest of content {}, offset {}, length {}", hash, offset, length);
        return contentManager.getDigest(hash, offset, length);
    }

    @Override
    public List<Event> history(boolean chronological, long first, int number) {
        ensureOpen();
        log("Returning history{}, first {}, count {}", chronological ? "" : ", reverse", first, number);
        return storageManager.inTransaction(() -> historyManager.history(chronological, first, number));
    }

    @Override
    public List<IndexEntry> find(String query, int first, int number) {
        ensureOpen();
        return index.find(query, first, number);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new RepositoryClosedException();
        }
    }

    private void log(String format, Object... args) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        LOG.info(on("").join("[", def.getName(), "] ", format), args);
    }
}
