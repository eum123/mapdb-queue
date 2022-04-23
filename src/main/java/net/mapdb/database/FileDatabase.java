package net.mapdb.database;

import net.mapdb.database.common.policy.CommitPolicy;
import net.mapdb.database.exception.UnsupportedClassType;
import net.mapdb.database.map.MMap;
import net.mapdb.database.map.MMapConfig;
import net.mapdb.database.map.MMapImpl;
import net.mapdb.database.queue.*;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FileDatabase<K, V> implements Database {
    private FileDatabaseConfig config ;
    private DB db;
    private List<ManagedStore> list = new ArrayList();

    public FileDatabase(FileDatabaseConfig config){
        this.config = config;
    }

    private ExecutorService executor;

    public void start() throws Exception {
        initDirectory(config.getFilePath());

        DBMaker.Maker maker = DBMaker.fileDB(Paths.get(config.getFilePath(), config.getFileName()).toFile());
        maker.transactionEnable();
        maker.fileChannelEnable();
        this.db = maker.make();

        //비동기 commit이면 별도 thread로 처리한다.
        if(config.getCommitPolicy().equals(CommitPolicy.ASYNC)) {

            executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> {
                db.commit();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}
            });
        }

    }

    public void close() throws Exception {
        try {
            if (executor != null) {
                try {
                    executor.shutdown();
                    //db close 전까지 thread를 종료하기 위해 대기한다.
                    //TODO: 기준 시간 설정으로 변경
                    executor.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e){
                }finally {
                    if(!executor.isTerminated()) {
                        executor.shutdownNow();
                    }
                }
            }

            //queue, map 종료 처리
            list.forEach(v -> {
                try {
                    v.shutdown();
                } catch (Exception e) {
                    //TODO: logging
                }
            });

        } finally {
            db.commit();
            db.close();
        }
    }

    /**
     * Queue create or open
     * @param queueName
     * @return
     */
    public MQueue<V> getQueue(String queueName) throws Exception {
        return getQueue(MQueueConfig.builder().valueType(String.class).queueName(queueName).build());
    }

    public MQueue<V> getQueue(MQueueConfig config) throws Exception {
        MQueue<V> queue = new MQueueImpl<V>(db, config);

        list.add(queue);    //shutdown에 사용

        return queue;
    }

    public MBlockQueue<V> getBlockQueue(String queueName) throws UnsupportedClassType {
       return getBlockQueue(MQueueConfig.builder().valueType(String.class).queueName(queueName).build());
    }

    /**
     * Blocking Queue create or open
     * @param config
     * @return
     */
    public MBlockQueue<V> getBlockQueue(MQueueConfig config) throws UnsupportedClassType {
        MBlockQueue<V> queue = new MBlockingQueueImpl(db, config);
        list.add(queue);    //shutdown에 사용
        return queue;
    }

    /**
     * Map create or open
     * @return
     */
    @Override
    public MMap<K, V> getMap(String name) throws UnsupportedClassType  {
        return getMap(MMapConfig.builder().mapName(name).keyType(String.class).valueType(String.class).build());
    }

    public MMap<K, V> getMap(MMapConfig config) throws UnsupportedClassType {
        MMap<K, V> map = new MMapImpl(db, config);
        list.add(map); //shutdown에 사용
        return map;
    }

    private void initDirectory(String path) throws IOException {
        Files.createDirectories(Paths.get(path));
    }

}
