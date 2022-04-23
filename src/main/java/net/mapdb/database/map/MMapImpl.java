package net.mapdb.database.map;

import net.mapdb.database.Database;
import net.mapdb.database.exception.UnsupportedClassType;
import org.mapdb.HTreeMap;

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
    private final Database db;
    private HTreeMap<K, Date> lifecycle;
    private HTreeMap<K, V> data;
    private final MMapConfig config;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private boolean isExpirationCheck = false;

    private ExecutorService executorService;

    public MMapImpl(Database db, HTreeMap<K, Date> lifecycle, HTreeMap<K, V> data, MMapConfig config) throws UnsupportedClassType {
        this.db = db;
        this.config = config;

        this.lifecycle = lifecycle;
        this.data = data;

        //expirer
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

                           if(Duration.between(start, now).getSeconds() > 5) {
                               lifecycle.remove(k);
                               config.getListener().onExpiration(data.remove(k));

                               db.commit();
                           } else {

                               //TODO: 중간에 멈추는지 확인
                               return;
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

            db.commit();
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
            V value = data.remove(key);
            db.commit();
            return value;
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

    public String getName() {
        return config.getMapName();
    }
}
