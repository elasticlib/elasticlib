package store.common;

import com.google.common.collect.ImmutableMap;
import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.TreeSet;
import org.joda.time.Instant;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.value.Value;

/**
 * Test data.
 */
public final class TestData {

    /**
     * Some ContentInfo instances.
     */
    public static final List<ContentInfo> CONTENT_INFOS = new ArrayList<>();
    /**
     * A ContentInfoTree instance.
     */
    public static final ContentInfoTree CONTENT_INFO_TREE;
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

    static {
        String[] HASHES = new String[]{"8d5f3c77e94a0cad3a32340d342135f43dbb7cbb",
                                       "0827c43f0aad546501f99b11f0bd44be42d68870",
                                       "39819150ee99549a8c0a59782169bb3be65b46a4"};

        String[] REVS = new String[]{"0d99dd9895a2a1c485e0c75f79f92cc14457bb62",
                                     "a0b87ac4b04a0bed394517d0b01792635531aa42"};

        CONTENT_INFOS.add(new ContentInfo.ContentInfoBuilder()
                .withContent(new Hash(HASHES[0]))
                .withLength(10)
                .with("text", Value.of("ipsum lorem"))
                .build(new Hash(REVS[0])));

        CONTENT_INFOS.add(new ContentInfo.ContentInfoBuilder()
                .withContent(new Hash(HASHES[1]))
                .withLength(120)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])));

        CONTENT_INFO_TREE = new ContentInfoTree.ContentInfoTreeBuilder()
                .add(CONTENT_INFOS.get(0))
                .add(new ContentInfo.ContentInfoBuilder()
                .withContent(new Hash(HASHES[0]))
                .withLength(10)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])))
                .build();

        EVENTS.add(new Event.EventBuilder()
                .withSeq(0)
                .withContent(new Hash(HASHES[0]))
                .withRevisions(new TreeSet<>(singleton(new Hash(REVS[0]))))
                .withTimestamp(new Instant(0))
                .withOperation(Operation.CREATE)
                .build());

        EVENTS.add(new Event.EventBuilder()
                .withSeq(1)
                .withContent(new Hash(HASHES[1]))
                .withRevisions(new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1]))))
                .withTimestamp(new Instant(123000))
                .withOperation(Operation.DELETE)
                .build());

        COMMAND_RESULTS.add(CommandResult.of(1,
                                             Operation.CREATE,
                                             new Hash(HASHES[0]),
                                             new TreeSet<>(singleton(new Hash(REVS[0])))));

        COMMAND_RESULTS.add(CommandResult.noOp(2,
                                               new Hash(HASHES[1]),
                                               new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[0]),
                                         new TreeSet<>(singleton(new Hash(REVS[0])))));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[1]),
                                         new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));

        Guid primaryId = new Guid("8d5f3c77e94a0cad3a32340d342135f4");
        Guid secondaryId = new Guid("0d99dd9895a2a1c485e0c75f79f92cc1");

        REPOSITORY_DEFS.add(new RepositoryDef("primary",
                                              primaryId,
                                              Paths.get("/repo/primary")));

        REPOSITORY_DEFS.add(new RepositoryDef("secondary",
                                              secondaryId,
                                              Paths.get("/repo/secondary")));

        REPLICATION_DEFS.add(new ReplicationDef(primaryId, secondaryId));
        REPLICATION_DEFS.add(new ReplicationDef(secondaryId, primaryId));

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
    }

    private TestData() {
    }
}
