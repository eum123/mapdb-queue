package net.mapdb.database.map;

import net.mapdb.database.ManagedStore;

public interface MMap<K, V> extends ManagedStore {
    void put(K key, V value);

    V get(K key);
}
