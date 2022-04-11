package net.mapdb.database.listener;

public interface ExpireListener<T> {
    void onExpiration(T data);
}
