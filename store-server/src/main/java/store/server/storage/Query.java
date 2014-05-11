package store.server.storage;

/**
 * Functionnal interface defining a query, that is a non-mutative operation.
 *
 * @param <T> Query return type.
 */
public interface Query<T> {

    /**
     * Invoke this query.
     *
     * @return Query result.
     */
    T apply();
}
