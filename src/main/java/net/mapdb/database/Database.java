package net.mapdb.database;

import net.mapdb.database.exception.UnsupportedClassType;
import net.mapdb.database.map.MMap;
import net.mapdb.database.map.MMapConfig;
import net.mapdb.database.queue.MBlockQueue;
import net.mapdb.database.queue.MQueue;
import net.mapdb.database.queue.MQueueConfig;

public interface Database<K, V> {
    void start() throws Exception;

    void close() throws Exception;

    void commit();

    /**
     * Queue create or open
     * @param queueName
     * @return
     */
    MQueue<V> getQueue(String queueName) throws Exception;

    MQueue<V> getQueue(MQueueConfig config) throws Exception;

    MBlockQueue<V> getBlockQueue(String queueName) throws UnsupportedClassType;
    /**
     * Blocking Queue create or open
     * @param config
     * @return
     */
    MBlockQueue<V> getBlockQueue(MQueueConfig config) throws UnsupportedClassType;

    /**
     * Map create or open
     * @return
     */
    MMap<K, V> getMap(String name) throws UnsupportedClassType;

    MMap<K, V> getMap(MMapConfig config) throws UnsupportedClassType;
}
