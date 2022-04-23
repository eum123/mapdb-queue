package net.mapdb.database.queue;

import net.mapdb.database.Database;
import net.mapdb.database.exception.UnsupportedClassType;
import net.mapdb.database.util.sequence.DatePrefixIntSequenceGenerator;
import net.mapdb.database.util.sequence.Sequence;
import org.mapdb.HTreeMap;

import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MBlockingQueueImpl<T> implements MBlockQueue<T> {
    private final Database db;
    private final MQueueConfig config;
    private NavigableSet<String> index;
    private HTreeMap<String, T> data;

    private Sequence sequence = new DatePrefixIntSequenceGenerator(10, 10);

    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    public MBlockingQueueImpl(Database db, NavigableSet<String> index, HTreeMap<String, T> data, MQueueConfig config) throws UnsupportedClassType {
        this.db = db;
        this.config = config;
        this.index = index;
        this.data = data;
    }

    /**
     * 가장 오래된 데이터를 조회 한다.
     * 반환할 데이터가 없는 경우 데이터가 추가 될때까지 대기 한다.
     * 데이터를 삭제 후 반환 한다.
     * @return
     */
    @Override
    public T poll() throws InterruptedException {
        lock.lock();

        try {
            if (index.size() == 0) {
                condition.await();
            }

            String key = index.pollFirst();
            return data.remove(key);
        }finally {
            lock.unlock();
        }
    }

    /**
     * 가장 오래된 데이터를 조회 한다.
     * 반환할 데이터가 없는 경우 정해진 시간까지 대기하고 정해진 시간 이후 데이터가 없는 경우 null 반환.
     * 데이터를 삭제 후 반환 한다.
     * @param timeout
     * @param unit
     * @return
     * @throws InterruptedException
     */
    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();

        try {
            if (index.size() == 0) {
                condition.await(timeout, unit);
            }

            String key = index.pollFirst();
            return data.remove(key);
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

            condition.signal();
        }finally {

            lock.unlock();
        }
    }

    @Override
    public void shutdown() throws Exception {
        lock.lock();

        try {
            condition.signalAll();
        }finally {
            lock.unlock();
        }
    }

    public String getName() {
        return config.getQueueName();
    }
}
