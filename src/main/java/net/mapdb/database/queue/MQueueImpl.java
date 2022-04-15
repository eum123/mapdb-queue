package net.mapdb.database.queue;

import java.util.NavigableSet;

public class MQueueImpl<T> implements MQueue<T> {
    private MQueueConfig config;
    private NavigableSet queue;
    public MQueueImpl(MQueueConfig config, NavigableSet queue) {
        this.queue = queue;

    }
    @Override
    public T poll() {
        return null;
    }
}
