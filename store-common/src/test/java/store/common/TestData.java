package store.common;

import com.google.common.collect.ImmutableMap;
import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.Collections;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.TreeSet;
import org.joda.time.Instant;
import store.common.exception.IOFailureException;
import store.common.exception.NodeException;
import store.common.exception.UnexpectedFailureException;
import store.common.exception.UnknownRepositoryException;
import store.common.hash.Digest.DigestBuilder;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.model.AgentInfo;
import store.common.model.AgentState;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentState;
import store.common.model.Event;
import store.common.model.Event.EventBuilder;
import store.common.model.IndexEntry;
import store.common.model.NodeDef;
import store.common.model.NodeInfo;
import store.common.model.Operation;
import store.common.model.ReplicationDef;
import store.common.model.ReplicationInfo;
import store.common.model.RepositoryDef;
import store.common.model.RepositoryInfo;
import store.common.model.RepositoryStats;
import store.common.model.Revision;
import store.common.model.Revision.RevisionBuilder;
import store.common.model.RevisionTree;
import store.common.model.RevisionTree.RevisionTreeBuilder;
import store.common.model.StagingInfo;
import store.common.value.Value;

/**
 * Test data.
 */
public final class TestData {

    /**
     * A StagingInfo instance.
     */
    public static final StagingInfo STAGING_INFO;
    /**
     * Some Revision instances.
     */
    public static final List<Revision> REVISIONS = new ArrayList<>();
    /**
     * A RevisionTree instance.
     */
    public static final RevisionTree REVISION_TREE;
    /**
     * A ContentInfo instance.
     */
    public static final ContentInfo CONTENT_INFO;
    /**
     * Some Event instances.
     */
    public static final List<Event> EVENTS = new ArrayList<>();
    /**
     * Some CommandResult instances.
     */
    public static final List<CommandResult> COMMAND_RESULTS = new ArrayList<>();
    /**
     * Some IndexEntry instances.
     */
    public static final List<IndexEntry> INDEX_ENTRIES = new ArrayList<>();
    /**
     * Some RepositoryDef instances.
     */
    public static final List<RepositoryDef> REPOSITORY_DEFS = new ArrayList<>();
    /**
     * Some ReplicationDef instances.
     */
    public static final List<ReplicationDef> REPLICATION_DEFS = new ArrayList<>();
    /**
     * Some RepositoryInfo instances.
     */
    public static final List<RepositoryInfo> REPOSITORY_INFOS = new ArrayList<>();
    /**
     * Some ReplicationInfo instances.
     */
    public static final List<ReplicationInfo> REPLICATION_INFOS = new ArrayList<>();
    /**
     * Some NodeDef instances.
     */
    public static final List<NodeDef> NODE_DEFS = new ArrayList<>();
    /**
     * Some NodeInfo instances.
     */
    public static final List<NodeInfo> NODE_INFOS = new ArrayList<>();
    /**
     * Some NodeException instances.
     */
    public static final List<NodeException> NODE_EXCEPTIONS = new ArrayList<>();

    static {
        String[] HASHES = new String[]{"8d5f3c77e94a0cad3a32340d342135f43dbb7cbb",
                                       "0827c43f0aad546501f99b11f0bd44be42d68870",
                                       "39819150ee99549a8c0a59782169bb3be65b46a4"};

        String[] REVS = new String[]{"0d99dd9895a2a1c485e0c75f79f92cc14457bb62",
                                     "a0b87ac4b04a0bed394517d0b01792635531aa42"};

        Guid[] GUIDS = new Guid[]{new Guid("8d5f3c77e94a0cad3a32340d342135f4"),
                                  new Guid("0d99dd9895a2a1c485e0c75f79f92cc1"),
                                  new Guid("39819150ee99549a8c0a59782169bb3b")};

        STAGING_INFO = new StagingInfo(GUIDS[0], new Hash(HASHES[1]), 123456L);

        REVISIONS.add(new RevisionBuilder()
                .withContent(new Hash(HASHES[0]))
                .withLength(10)
                .with("text", Value.of("ipsum lorem"))
                .build(new Hash(REVS[0])));

        REVISIONS.add(new RevisionBuilder()
                .withContent(new Hash(HASHES[1]))
                .withLength(120)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])));

        REVISION_TREE = new RevisionTreeBuilder()
                .add(REVISIONS.get(0))
                .add(new RevisionBuilder()
                .withContent(new Hash(HASHES[0]))
                .withLength(10)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])))
                .build();

        CONTENT_INFO = new ContentInfo(ContentState.PRESENT,
                                       new DigestBuilder().getHash(),
                                       0,
                                       singletonList(REVISIONS.get(0)));

        EVENTS.add(new EventBuilder()
                .withSeq(0)
                .withContent(new Hash(HASHES[0]))
                .withRevisions(new TreeSet<>(singleton(new Hash(REVS[0]))))
                .withTimestamp(new Instant(0))
                .withOperation(Operation.CREATE)
                .build());

        EVENTS.add(new EventBuilder()
                .withSeq(1)
                .withContent(new Hash(HASHES[1]))
                .withRevisions(new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1]))))
                .withTimestamp(new Instant(123000))
                .withOperation(Operation.DELETE)
                .build());

        COMMAND_RESULTS.add(CommandResult.of(Operation.CREATE,
                                             new Hash(HASHES[0]),
                                             new TreeSet<>(singleton(new Hash(REVS[0])))));

        COMMAND_RESULTS.add(CommandResult.noOp(new Hash(HASHES[1]),
                                               new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[0]),
                                         new TreeSet<>(singleton(new Hash(REVS[0])))));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[1]),
                                         new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));

        REPOSITORY_DEFS.add(new RepositoryDef("primary",
                                              GUIDS[0],
                                              Paths.get("/repo/primary")));

        REPOSITORY_DEFS.add(new RepositoryDef("secondary",
                                              GUIDS[1],
                                              Paths.get("/repo/secondary")));

        REPLICATION_DEFS.add(new ReplicationDef(GUIDS[0], GUIDS[1]));
        REPLICATION_DEFS.add(new ReplicationDef(GUIDS[1], GUIDS[0]));

        REPOSITORY_INFOS.add(new RepositoryInfo(REPOSITORY_DEFS.get(0),
                                                new RepositoryStats(12, 3, 2, ImmutableMap.of("contentType", 9L,
                                                                                              "description", 5L)),
                                                new AgentInfo(15, 17, AgentState.RUNNING),
                                                new AgentInfo(17, 17, AgentState.WAITING)));

        REPOSITORY_INFOS.add(new RepositoryInfo(REPOSITORY_DEFS.get(1)));

        REPLICATION_INFOS.add(new ReplicationInfo(REPOSITORY_DEFS.get(0),
                                                  REPOSITORY_DEFS.get(1),
                                                  new AgentInfo(15, 17, AgentState.RUNNING)));

        REPLICATION_INFOS.add(new ReplicationInfo(REPOSITORY_DEFS.get(0),
                                                  REPOSITORY_DEFS.get(1)));

        NODE_DEFS.add(new NodeDef("alpha",
                                  GUIDS[0],
                                  singletonList(URI.create("http://192.168.0.1:8080"))));

        NODE_DEFS.add(new NodeDef("beta",
                                  GUIDS[1],
                                  asList(URI.create("http://192.168.0.2:8080"),
                                         URI.create("http://31.34.134.14:8080"))));

        NODE_DEFS.add(new NodeDef("gamma",
                                  GUIDS[2],
                                  Collections.<URI>emptyList()));

        NODE_INFOS.add(new NodeInfo(NODE_DEFS.get(0),
                                    new Instant(0)));

        NODE_INFOS.add(new NodeInfo(NODE_DEFS.get(1),
                                    NODE_DEFS.get(1).getPublishUris().get(1),
                                    new Instant(123000)));

        NODE_EXCEPTIONS.add(new UnknownRepositoryException());
        NODE_EXCEPTIONS.add(new UnexpectedFailureException(new NullPointerException()));
        NODE_EXCEPTIONS.add(new IOFailureException(new FileNotFoundException("/tmp/test")));
    }

    private TestData() {
    }
}
