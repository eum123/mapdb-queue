package net.mapdb.database.map;

import lombok.extern.slf4j.Slf4j;
import net.mapdb.database.Database;
import net.mapdb.database.exception.UnsupportedClassType;
import org.mapdb.HTreeMap;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class MMapImpl<K, V> implements MMap<K, V> {
    private final Database db;
    private HTreeMap<K, Long> lifecycle;
    private HTreeMap<K, V> data;
    private final MMapConfig config;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private boolean isExpirationCheck = false;

    private ExecutorService executorService;
    private boolean isStart = true;

    public MMapImpl(Database db, HTreeMap<K, Long> lifecycle, HTreeMap<K, V> data, MMapConfig config) throws UnsupportedClassType {
        this.db = db;
        this.config = config;

        this.lifecycle = lifecycle;
        this.data = data;

        //expirer
        if(this.config.getListener() != null) {
            executorService = Executors.newSingleThreadExecutor();
            executorService.submit(()-> {
                while(isStart) {
                    lock.lock();
                    try {
                        isExpirationCheck = true;

                        data.keySet().forEach(k -> {
                            Long date = lifecycle.get(k);

                            if (date != null) {
                                LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(date), TimeZone.getDefault().toZoneId());
                                LocalDateTime now = LocalDateTime.now();

                                if (Duration.between(start, now).getSeconds() > config.getExpirationInterval()) {
                                    lifecycle.remove(k);

                                    try {
                                        config.getListener().onExpiration(data.remove(k));
                                    } catch (Exception e) {
                                        log.error("listener error", e);
                                    }

                                    db.commit();
                                }
                            }
                        });
                    } finally {
                        isExpirationCheck = false;
                        condition.signalAll();
                        lock.unlock();
                    }

                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                    }
                }
            });
        }
    }

    public void put(K key, V value) {
        lock.lock();
        try {
            lifecycle.put(key, System.currentTimeMillis());
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
        isStart = false;
        if (executorService != null) {

            if (executorService != null) {
                executorService.shutdownNow();

                executorService.awaitTermination(5, TimeUnit.SECONDS);
                log.info("SHUTDOWN expiration worker (isTerminated:{})", executorService.isTerminated());
            }
        }
    }

    public int size() {
        return data.size();
    }

    public String getName() {
        return config.getMapName();
    }
}
