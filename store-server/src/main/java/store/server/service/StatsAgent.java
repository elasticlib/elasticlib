package store.server.service;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.RepositoryStats;
import store.common.hash.Hash;

/**
 * An agent that computes statistics about a repository.
 */
class StatsAgent extends Agent {

    private final Repository repository;
    private final StatsManager statsManager;

    public StatsAgent(Repository repository,
                      StatsManager statsManager,
                      Database curSeqsDb,
                      DatabaseEntry curSeqKey) {
        super("stats-" + repository.getDef().getName(), curSeqsDb, curSeqKey);
        this.repository = repository;
        this.statsManager = statsManager;
    }

    @Override
    protected List<Event> history(boolean chronological, long first, int number) {
        return repository.history(chronological, first, number);
    }

    @Override
    protected boolean process(Event event) {
        statsManager.update(computeDiff(event));
        return true;
    }

    private RepositoryStats computeDiff(Event event) {
        RepositoryStats current = statsManager.stats();
        switch (event.getOperation()) {
            case CREATE:
                return new RepositoryStats(current.getCreations() + 1,
                                           current.getUpdates(),
                                           current.getDeletions(),
                                           add(current.getMetadataCounts(),
                                               counts(repository.getContentInfoHead(event.getContent()))));
            case UPDATE:
                return new RepositoryStats(current.getCreations(),
                                           current.getUpdates() + 1,
                                           current.getDeletions(),
                                           add(current.getMetadataCounts(), diff(event)));

            case DELETE:
                return new RepositoryStats(current.getCreations(),
                                           current.getUpdates(),
                                           current.getDeletions() + 1,
                                           add(current.getMetadataCounts(), diff(event)));
            default:
                throw new AssertionError();
        }
    }

    private static Map<String, Long> add(Map<String, Long> base, Map<String, Long> diff) {
        return reduce(base, diff, true);
    }

    private static Map<String, Long> substract(Map<String, Long> base, Map<String, Long> diff) {
        return reduce(base, diff, false);
    }

    private static Map<String, Long> reduce(Map<String, Long> base, Map<String, Long> diff, boolean add) {
        Map<String, Long> counts = new TreeMap<>();
        counts.putAll(base);
        for (Entry<String, Long> entry : diff.entrySet()) {
            String key = entry.getKey();
            long baseValue = counts.containsKey(key) ? counts.get(key) : 0;
            long diffValue = (add ? 1 : -1) * entry.getValue();
            long value = baseValue + diffValue;
            if (value == 0) {
                counts.remove(key);
            } else {
                counts.put(key, value);
            }
        }
        return counts;
    }

    private static Map<String, Long> counts(List<ContentInfo> revisions) {
        Map<String, Long> counts = new TreeMap<>();
        for (ContentInfo info : revisions) {
            for (String key : info.getMetadata().keySet()) {
                long value = counts.containsKey(key) ? counts.get(key) + 1 : 1;
                counts.put(key, value);
            }
        }
        return counts;
    }

    private Map<String, Long> diff(Event event) {
        ContentInfoTree tree = repository.getContentInfoTree(event.getContent());
        return substract(counts(tree.get(tree.getHead())),
                         counts(tree.get(previousHead(event))));
    }

    private SortedSet<Hash> previousHead(Event latest) {
        // Performance : We make a full-scan over the history there !
        // A secondary index on the events per content would be welcome.

        long seq = latest.getSeq() - 1;
        while (seq > 0) {
            for (Event event : repository.history(false, seq, 100)) {
                if (event.getContent().equals(latest.getContent())) {
                    return event.getRevisions();
                }
                seq = event.getSeq() - 1;
            }
        }
        // Should not happen.
        throw new AssertionError();
    }
}
