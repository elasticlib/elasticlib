package store.server.transaction;

public interface Query<T> {

    T apply();
}
