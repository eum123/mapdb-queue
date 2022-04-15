package net.mapdb.database;

import net.mapdb.database.map.MMap;
import net.mapdb.database.queue.MBlockQueue;
import net.mapdb.database.queue.MQueue;

public interface Database {
    MQueue getQueue(String queueName) throws Exception;
    MBlockQueue getBlockQueue(String queueName) throws Exception;
    MMap getMap() throws Exception;

    void start() throws Exception;

    void close() throws Exception;
}
