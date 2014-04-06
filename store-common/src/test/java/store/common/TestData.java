package store.common;

import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.TreeSet;
import org.joda.time.Instant;
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
     * A Config instance.
     */
    public static final Config CONFIG;
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

    static {
        String[] HASHES = new String[]{"8d5f3c77e94a0cad3a32340d342135f43dbb7cbb",
                                       "0827c43f0aad546501f99b11f0bd44be42d68870",
                                       "39819150ee99549a8c0a59782169bb3be65b46a4"};

        String[] REVS = new String[]{"0d99dd9895a2a1c485e0c75f79f92cc14457bb62",
                                     "a0b87ac4b04a0bed394517d0b01792635531aa42"};

        CONTENT_INFOS.add(new ContentInfo.ContentInfoBuilder()
                .withHash(new Hash(HASHES[0]))
                .withLength(10)
                .with("text", Value.of("ipsum lorem"))
                .build(new Hash(REVS[0])));

        CONTENT_INFOS.add(new ContentInfo.ContentInfoBuilder()
                .withHash(new Hash(HASHES[1]))
                .withLength(120)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])));

        CONTENT_INFO_TREE = new ContentInfoTree.ContentInfoTreeBuilder()
                .add(CONTENT_INFOS.get(0))
                .add(new ContentInfo.ContentInfoBuilder()
                .withHash(new Hash(HASHES[0]))
                .withLength(10)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])))
                .build();

        CONFIG = new Config();
        CONFIG.addRepository(Paths.get("/repo/primary"));
        CONFIG.addRepository(Paths.get("/repo/secondary"));
        CONFIG.addReplication("primary", "secondary");

        EVENTS.add(new Event.EventBuilder()
                .withSeq(0)
                .withHash(new Hash(HASHES[0]))
                .withHead(new TreeSet<>(singleton(new Hash(REVS[0]))))
                .withTimestamp(new Instant(0))
                .withOperation(Operation.CREATE)
                .build());

        EVENTS.add(new Event.EventBuilder()
                .withSeq(1)
                .withHash(new Hash(HASHES[1]))
                .withHead(new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1]))))
                .withTimestamp(new Instant(123000))
                .withOperation(Operation.DELETE)
                .build());

        COMMAND_RESULTS.add(CommandResult.of(1, Operation.CREATE, new TreeSet<>(singleton(new Hash(REVS[0])))));
        COMMAND_RESULTS.add(CommandResult.noOp(2, new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[0]),
                                         new TreeSet<>(singleton(new Hash(REVS[0])))));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[1]),
                                         new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));
    }

    private TestData() {
    }
}
