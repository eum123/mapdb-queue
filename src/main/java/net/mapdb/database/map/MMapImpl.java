package net.mapdb.database.map;

import net.mapdb.database.listener.ExpireListener;
import org.mapdb.HTreeMap;

public class MMapImpl<K, V> implements MMap<K, V> {

    private HTreeMap<K, V> map;
    private ExpireListener listener = null;

    public MMapImpl(HTreeMap<K, V> map) {
        this.map = map;
    }

    public MMapImpl(HTreeMap<K, V> map, ExpireListener listener) {
        this.map = map;
        this.listener = listener;
    }

    public void put(K key, V value) {
        map.put(key, value);
    }

    public V get(K key) {
        return map.get(key);
    }
}
