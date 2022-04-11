package net.mapdb.database.queue;

public interface MQueue<T> {
    public T poll();
}
