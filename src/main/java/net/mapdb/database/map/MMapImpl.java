package net.mapdb.database.map;

import net.mapdb.database.exception.UnsupportedClassType;
import net.mapdb.database.util.GroupSerializerHelper;
import org.mapdb.DB;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MMapImpl<K, V> implements MMap<K, V> {

    private HTreeMap<K, Date> lifecycle;
    private HTreeMap<K, V> data;
    private final MMapConfig config;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private boolean isExpirationCheck = false;

    private ExecutorService executorService;

    public MMapImpl(DB db, MMapConfig config) throws UnsupportedClassType {
        this.config = config;

        GroupSerializer<K> keySerializer = GroupSerializerHelper.convertClassToGroupSerializer(config.getKeyType());
        GroupSerializer<V> ValueSerializer = GroupSerializerHelper.convertClassToGroupSerializer(config.getValueType());

        data = db.hashMap(config.getMapName())
                .keySerializer(keySerializer)
                .valueSerializer(ValueSerializer)
                .createOrOpen();

        lifecycle = db.hashMap(config.getMapName())
                .keySerializer(keySerializer)
                .valueSerializer(Serializer.DATE)
                .createOrOpen();

        if(this.config.getListener() != null) {
            executorService = Executors.newSingleThreadExecutor();
            executorService.submit(()-> {
                lock.lock();
                try {
                   isExpirationCheck = true;

                   data.keySet().forEach(k -> {
                       Date date = lifecycle.get(k);
                       if(date != null) {
                           LocalDateTime start = new Date().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                           LocalDateTime now = LocalDateTime.now();

                           //TODO : 설정으로 전환
                           if(Duration.between(start, now).getSeconds() > 5) {
                               lifecycle.remove(k);
                               config.getListener().onExpiration(data.remove(k));
                           }
                       }
                   });

                }finally {
                   isExpirationCheck = false;
                   lock.unlock();
                }

                try {
                   //TODO: TimeUnit 설정으로 전환
                   TimeUnit.SECONDS.sleep(config.getInterval());
                } catch(InterruptedException e){

                }
            });
        }
    }

    public void put(K key, V value) {
        lock.lock();
        try {
           lifecycle.put(key, new Date());
            data.put(key, value);
        } finally {
            lock.unlock();
        }
        data.put(key, value);
    }

    public V get(K key) {
        lock.lock();
        try {
            if (isExpirationCheck) {
                condition.await();
            }
            return data.remove(key);
        } catch (InterruptedException e) {
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() throws Exception {

        if (executorService != null) {

            try {
                executorService.shutdown();

                //TODO: 기준 시간 설정으로 변경
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e){
            }finally {
                if(!executorService.isTerminated()) {
                    executorService.shutdownNow();
                }
            }
        }

    }
}
