package store.common;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import store.common.value.Value;

final class MappableUtil {

    private MappableUtil() {
    }

    public static List<Value> toList(SortedSet<Hash> hashes) {
        List<Value> values = new ArrayList<>();
        for (Hash hash : hashes) {
            values.add(Value.of(hash.getBytes()));
        }
        return values;
    }

    public static SortedSet<Hash> fromList(List<Value> values) {
        SortedSet<Hash> hashes = new TreeSet<>();
        for (Value value : values) {
            hashes.add(new Hash(value.asByteArray()));
        }
        return hashes;
    }
}
