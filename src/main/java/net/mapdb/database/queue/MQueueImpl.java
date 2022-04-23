package net.mapdb.database.queue;

import net.mapdb.database.exception.UnsupportedClassType;
import net.mapdb.database.util.GroupSerializerHelper;
import net.mapdb.database.util.sequence.DatePrefixIntSequenceGenerator;
import net.mapdb.database.util.sequence.Sequence;
import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;

import java.util.NavigableSet;
import java.util.concurrent.locks.ReentrantLock;

public class MQueueImpl<T> implements MQueue<T> {
    private final MQueueConfig config;
    private NavigableSet<String> index;
    private HTreeMap<String, T> data;

    private Sequence sequence = new DatePrefixIntSequenceGenerator(10, 10);

    private ReentrantLock lock = new ReentrantLock();

    public MQueueImpl(DB db, MQueueConfig config) throws UnsupportedClassType {
        this.config = config;

        this.index = db.treeSet(config.getQueueName() + "_index", Serializer.STRING).createOrOpen();

        GroupSerializer<T> serializer = GroupSerializerHelper.convertClassToGroupSerializer(config.getValueType());
        data = db.hashMap(config.getQueueName())
                .keySerializer(Serializer.STRING)
                .valueSerializer(serializer)
                .createOrOpen();


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
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() throws Exception {
        //empty
    }
}
