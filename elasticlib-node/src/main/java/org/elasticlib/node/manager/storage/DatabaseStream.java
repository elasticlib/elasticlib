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
package org.elasticlib.node.manager.storage;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.elasticlib.common.mappable.Mappable;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asMappable;

/**
 * A stream on a Berkeley DB database.
 *
 * Opens a cursor on the database and iterates on its records, performing a given action for each record's value.
 *
 * @param <T> Database values type.
 */
public class DatabaseStream<T extends Mappable> {

    private final StorageManager storageManager;
    private final Database database;
    private final Class<T> clazz;

    /**
     * Constructor.
     *
     * @param storageManager Storage manager.
     * @param database Database to stream.
     * @param clazz Database values class.
     */
    DatabaseStream(StorageManager storageManager, Database database, Class<T> clazz) {
        this.storageManager = storageManager;
        this.database = database;
        this.clazz = clazz;
    }

    private boolean each(LockMode lockMode, BiPredicate<Cursor, T> predicate) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        try (Cursor cursor = storageManager.openCursor(database)) {
            while (cursor.getNext(key, value, lockMode) == OperationStatus.SUCCESS) {
                if (predicate.test(cursor, asMappable(value, clazz))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Performs the supplied action for each value of this stream until all values have been processed.
     *
     * @param action The action to be performed.
     */
    public void each(Consumer<T> action) {
        each(LockMode.DEFAULT, (cursor, value) -> {
            action.accept(value);
            return false;
        });
    }

    /**
     * Performs the supplied action for each value of this stream until all values have been processed.
     *
     * @param action The action to be performed.
     */
    public void each(BiConsumer<Cursor, T> action) {
        each(LockMode.RMW, (cursor, value) -> {
            action.accept(cursor, value);
            return false;
        });
    }

    /**
     * Applies the supplied predicate to each value of this stream until all values have been evaluated or the predicate
     * short-circuits the stream by returning {@code true}.
     *
     * @param predicate The predicate to be applied.
     * @return If at least a value matches the supplied predicate.
     */
    public boolean any(Predicate<T> predicate) {
        return each(LockMode.DEFAULT, (cursor, value) -> predicate.test(value));
    }

    /**
     * Applies the supplied predicate to each value of this stream until all values have been evaluated or the predicate
     * short-circuits the stream by returning {@code true}.
     *
     * @param predicate The predicate to be applied.
     * @return If at least a value matches the supplied predicate.
     */
    public boolean any(BiPredicate<Cursor, T> predicate) {
        return each(LockMode.RMW, predicate);
    }

    /**
     * Provides the first value in the stream that matches supplied predicate, if any.
     *
     * @param predicate The predicate to be applied.
     * @return The first matching value, if any.
     */
    public Optional<T> first(Predicate<T> predicate) {
        List<T> list = new ArrayList<>(1);
        any(value -> {
            if (predicate.test(value)) {
                list.add(value);
                return true;
            }
            return false;
        });
        return list.stream().findFirst();
    }
}
