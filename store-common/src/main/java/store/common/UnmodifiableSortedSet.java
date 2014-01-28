package store.common;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;

/**
 * A non-modifiable sorted set.
 *
 * @param <T> item type
 */
public class UnmodifiableSortedSet<T> implements SortedSet<T> {

    private final SortedSet<T> delegate;

    /**
     * Constructeur.
     *
     * @param delegate Set to wrap.
     */
    public UnmodifiableSortedSet(SortedSet<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Comparator<? super T> comparator() {
        return delegate.comparator();
    }

    @Override
    public SortedSet<T> subSet(T fromElement, T toElement) {
        return delegate.subSet(fromElement, toElement);
    }

    @Override
    public SortedSet<T> headSet(T toElement) {
        return delegate.headSet(toElement);
    }

    @Override
    public SortedSet<T> tailSet(T fromElement) {
        return delegate.tailSet(fromElement);
    }

    @Override
    public T first() {
        return delegate.first();
    }

    @Override
    public T last() {
        return delegate.last();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return delegate.toArray(a);
    }

    @Override
    public boolean add(T e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
