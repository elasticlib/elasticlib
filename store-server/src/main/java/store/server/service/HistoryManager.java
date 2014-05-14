package store.server.service;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import static java.lang.Math.min;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.SortedSet;
import org.joda.time.Instant;
import store.common.Event;
import store.common.Operation;
import store.common.bson.BsonWriter;
import store.common.hash.Hash;
import static store.server.storage.DatabaseEntries.asLong;
import static store.server.storage.DatabaseEntries.asMappable;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.StorageManager;

class HistoryManager {

    private static final String HISTORY = "history";
    private final StorageManager storageManager;
    private final Database database;
    private final Sequence sequence;

    public HistoryManager(StorageManager storageManager) {
        this.storageManager = storageManager;
        this.database = storageManager.openDatabase(HISTORY);
        this.sequence = storageManager.openSequence(HISTORY);
    }

    public void add(Operation operation, Hash content, SortedSet<Hash> revisions) {
        long seq = sequence.get(null, 1);

        Event event = new Event.EventBuilder()
                .withSeq(seq)
                .withContent(content)
                .withRevisions(revisions)
                .withTimestamp(new Instant())
                .withOperation(operation)
                .build();

        database.put(StorageManager.currentTransaction(),
                     entry(seq),
                     new DatabaseEntry(new BsonWriter().put(event.toMap()).build()));
    }

    public List<Event> history(boolean chronological, long first, int number) {
        try (Cursor cursor = storageManager.openCursor(database)) {
            List<Event> events = new ArrayList<>(min(number, 1000));
            DatabaseEntry key = entry(first);
            DatabaseEntry data = new DatabaseEntry();
            if (cursor.getSearchKey(key, data, LockMode.DEFAULT) != OperationStatus.SUCCESS) {
                key.setData(null);
                if (!searchFirst(chronological, first, cursor, key, data)) {
                    return emptyList();
                }
            }
            events.add(asMappable(data, Event.class));
            while (events.size() < number && searchNext(chronological, cursor, key, data)) {
                events.add(asMappable(data, Event.class));
            }
            return events;
        }
    }

    private static boolean searchFirst(boolean chronological, long first,
                                       Cursor cursor, DatabaseEntry key, DatabaseEntry data) {
        if (chronological) {
            return cursor.getFirst(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS && first <= asLong(key);
        } else {
            return cursor.getLast(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS && first >= asLong(key);
        }
    }

    private static boolean searchNext(boolean chronological, Cursor cursor, DatabaseEntry key, DatabaseEntry data) {
        if (chronological) {
            return cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        } else {
            return cursor.getPrev(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS;
        }
    }
}
