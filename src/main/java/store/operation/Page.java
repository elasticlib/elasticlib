package store.operation;

import java.util.HashSet;
import java.util.Set;
import store.hash.Hash;

final class Page {

    private final Set<Hash> set = new HashSet<>();

    public boolean add(Hash hash) {
        return set.add(hash);
    }

    public void remove(Hash hash) {
        set.remove(hash);
    }
}
