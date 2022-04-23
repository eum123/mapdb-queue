package net.mapdb.database;

import lombok.extern.slf4j.Slf4j;
import net.mapdb.database.common.policy.CommitPolicy;
import net.mapdb.database.exception.UnsupportedClassType;
import net.mapdb.database.map.MMap;
import net.mapdb.database.map.MMapConfig;
import net.mapdb.database.map.MMapImpl;
import net.mapdb.database.queue.*;
import net.mapdb.database.util.GroupSerializerHelper;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
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
                log.info("COMMIT !!");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}
            });
        }
        log.info("START FileDatabase {}/{}", config.getFilePath(), config.getFileName());
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
                    log.info("SHUTDOWN [{}] ...", v.getName());
                } catch (Exception e) {
                    //TODO: logging
                }
            });

        } finally {
            db.commit();
            db.close();

            log.info("SHUTDOWN complete!!!");
        }
    }

    public void commit() {
        if (!config.getCommitPolicy().equals(CommitPolicy.ASYNC)) {
            db.commit();
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
        NavigableSet<String> index = db.treeSet(config.getQueueName() + "_index", Serializer.STRING).createOrOpen();

        GroupSerializer<V> serializer = GroupSerializerHelper.convertClassToGroupSerializer(config.getValueType());
        HTreeMap<String, V> data = db.hashMap(config.getQueueName())
                .keySerializer(Serializer.STRING)
                .valueSerializer(serializer)
                .createOrOpen();

        MQueue<V> queue = new MQueueImpl<V>(this, index, data, config);

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
        NavigableSet<String> index = db.treeSet(config.getQueueName() + "_index", Serializer.STRING).createOrOpen();

        GroupSerializer<V> serializer = GroupSerializerHelper.convertClassToGroupSerializer(config.getValueType());
        HTreeMap<String, V> data = db.hashMap(config.getQueueName())
                .keySerializer(Serializer.STRING)
                .valueSerializer(serializer)
                .createOrOpen();


        MBlockQueue<V> queue = new MBlockingQueueImpl(this, index, data, config);
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

        GroupSerializer<K> keySerializer = GroupSerializerHelper.convertClassToGroupSerializer(config.getKeyType());
        GroupSerializer<V> ValueSerializer = GroupSerializerHelper.convertClassToGroupSerializer(config.getValueType());

        HTreeMap<K, V> data = db.hashMap(config.getMapName())
                .keySerializer(keySerializer)
                .valueSerializer(ValueSerializer)
                .createOrOpen();

        HTreeMap<K, Date> lifecycle = db.hashMap(config.getMapName())
                .keySerializer(keySerializer)
                .valueSerializer(Serializer.DATE)
                .createOrOpen();

        MMap<K, V> map = new MMapImpl(this, lifecycle, data, config);
        list.add(map); //shutdown에 사용
        return map;
    }

    private void initDirectory(String path) throws IOException {
        Files.createDirectories(Paths.get(path));
    }

}
