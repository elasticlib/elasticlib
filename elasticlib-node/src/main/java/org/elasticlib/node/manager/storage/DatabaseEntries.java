package org.elasticlib.node.manager.storage;

import com.google.common.base.Charsets;
import com.sleepycat.je.DatabaseEntry;
import java.nio.ByteBuffer;
import org.elasticlib.common.bson.BsonReader;
import org.elasticlib.common.bson.BsonWriter;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.mappable.MappableUtil;

/**
 * Utilities to wrap/unwrap data to or from Berkeley DB database entries.
 */
public final class DatabaseEntries {

    private DatabaseEntries() {
    }

    /**
     * Builds a new database entry wrapping a hash.
     *
     * @param hash Value to wrap.
     * @return A new database entry.
     */
    public static DatabaseEntry entry(Hash hash) {
        return new DatabaseEntry(hash.getBytes());
    }

    /**
     * Builds a new database entry wrapping a GUID.
     *
     * @param guid Value to wrap.
     * @return A new database entry.
     */
    public static DatabaseEntry entry(Guid guid) {
        return new DatabaseEntry(guid.getBytes());
    }

    /**
     * Builds a new database entry wrapping a string.
     *
     * @param val Value to wrap.
     * @return A new database entry.
     */
    public static DatabaseEntry entry(String val) {
        return new DatabaseEntry(val.getBytes(Charsets.UTF_8));
    }

    /**
     * Builds a new database entry wrapping a pair of GUID.
     *
     * @param val1 First value of the pair to wrap.
     * @param val2 second value of the pair to wrap.
     * @return A new database entry.
     */
    public static DatabaseEntry entry(Guid val1, Guid val2) {
        return new DatabaseEntry(new BsonWriter()
                .put("1", val1)
                .put("2", val2)
                .build());
    }

    /**
     * Builds a new database entry wrapping a long.
     *
     * @param val Value to wrap.
     * @return A new database entry.
     */
    public static DatabaseEntry entry(long val) {
        return new DatabaseEntry(ByteBuffer.allocate(8).putLong(val).array());
    }

    /**
     * Builds a new database entry wrapping a mappable instance.
     *
     * @param mappable Value to wrap.
     * @return A new database entry.
     */
    public static DatabaseEntry entry(Mappable mappable) {
        return new DatabaseEntry(new BsonWriter().put(mappable.toMap()).build());
    }

    /**
     * Unwraps a long from supplied database entry.
     *
     * @param entry A database entry.
     * @return Wrapped long.
     */
    public static long asLong(DatabaseEntry entry) {
        return ByteBuffer.wrap(entry.getData()).getLong();
    }

    /**
     * Unwraps a GUID from supplied database entry.
     *
     * @param entry A database entry.
     * @return Wrapped GUID.
     */
    public static Guid asGuid(DatabaseEntry entry) {
        return new Guid(entry.getData());
    }

    /**
     * Unwraps a mappable instance from supplied database entry.
     *
     * @param <T> Actual class to convert to.
     * @param entry A database entry.
     * @param clazz Actual class to convert to.
     * @return Wrapped mappable instance.
     */
    public static <T extends Mappable> T asMappable(DatabaseEntry entry, Class<T> clazz) {
        return MappableUtil.fromMap(new BsonReader(entry.getData()).asMap(), clazz);
    }
}