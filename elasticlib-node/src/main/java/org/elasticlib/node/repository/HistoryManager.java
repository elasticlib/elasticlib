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

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import static java.lang.Math.min;
import static java.time.Instant.now;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.SortedSet;
import org.elasticlib.common.bson.BsonWriter;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.Operation;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asLong;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asMappable;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Creates and provides access to events of a repository.
 */
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
                .withTimestamp(now())
                .withOperation(operation)
                .build();

        database.put(storageManager.currentTransaction(),
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
