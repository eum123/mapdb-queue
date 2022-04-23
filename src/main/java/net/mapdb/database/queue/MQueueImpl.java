package net.mapdb.database.queue;

import net.mapdb.database.Database;
import net.mapdb.database.exception.UnsupportedClassType;
import net.mapdb.database.util.sequence.DatePrefixIntSequenceGenerator;
import net.mapdb.database.util.sequence.Sequence;
import org.mapdb.HTreeMap;

import java.util.NavigableSet;
import java.util.concurrent.locks.ReentrantLock;

public class MQueueImpl<T> implements MQueue<T> {
    private final Database db;
    private final MQueueConfig config;
    private NavigableSet<String> index;
    private HTreeMap<String, T> data;

    private Sequence sequence = new DatePrefixIntSequenceGenerator(10, 10);

    private ReentrantLock lock = new ReentrantLock();

    public MQueueImpl(Database db, NavigableSet<String> index, HTreeMap<String, T> data, MQueueConfig config) throws UnsupportedClassType {
        this.db = db;
        this.config = config;
        this.index = index;
        this.data = data;
    }

    /**
     * 가장 오래된 데이터를 조회 한다.
     * 데이터를 삭제 후 반환 한다.
     * @return
     */
    @Override
    public T poll() {
        lock.lock();

        try {
            String key = index.pollFirst();
            T value = data.remove(key);
            db.commit();

            return value;
        }finally {
            lock.unlock();
        }
    }

    /**
     * 저장되어 있는 데이터 개수를 구한다.
     * @return
     */
    @Override
    public long size() {
        return index.size();
    }

    /**
     * 데이터를 저장한다.
     * @param value
     */
    public void push(T value) {
        lock.lock();
        try {

            String key = sequence.nextValue();
            index.add(key);
            data.put(key, value);

            db.commit();
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() throws Exception {}

    public String getName() {
        return config.getQueueName();
    }
}
