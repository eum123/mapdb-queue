package net.mapdb.database;

import net.mapdb.database.common.policy.CommitPolicy;
import net.mapdb.database.map.MMap;
import net.mapdb.database.queue.MBlockQueue;
import net.mapdb.database.queue.MQueue;
import net.mapdb.database.queue.MQueueConfig;
import net.mapdb.database.queue.MQueueImpl;
import net.mapdb.database.util.GroupSerializerHelper;
import org.checkerframework.checker.units.qual.K;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;

import java.nio.file.Paths;
import java.util.NavigableSet;

public class FileDatabase<T> implements Database {
    private FileDatabaseConfig config ;
    private DB db;

    public FileDatabase(FileDatabaseConfig config){
        this.config = config;
    }

    private CommitWorker worker = null;

    public void start() throws Exception {
        checkDirectory(config.getFilePath());

        DBMaker.Maker maker = DBMaker.fileDB(Paths.get(config.getFilePath(), config.getFileName()).toFile());
        maker.transactionEnable();
        maker.fileChannelEnable();
        this.db = maker.make();

        //비동기 commit이면 별도 thread로 처리한다.
        if(config.getCommitPolicy().equals(CommitPolicy.ASYNC)) {
            worker = new CommitWorker();
            worker.start();
        }

    }

    public void close() throws Exception {
        try {
            if (worker != null) {
                worker.terminate();

                //db close 전까지 thread를 종료하기 위해 대기한다.
                while (worker.isAlive()) {
                    //TODO : logging worker alive message
                    Thread.sleep(1000);
                }
                //stop worker
            }

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
    public MQueue<T> getQueue(String queueName) throws Exception {
        return getQueue(MQueueConfig.builder().queueName(queueName).valueType(String.class).build());
    }

    public MQueue<T> getQueue(MQueueConfig config) throws Exception {

        GroupSerializer<T> serializer = GroupSerializerHelper.convertClassToGroupSerializer(config.getValueType());

        return new MQueueImpl<T>(config, db.treeSet(config.getQueueName(), serializer).createOrOpen());
    }

    /**
     * Blocking Queue create or open
     * @param queueName
     * @return
     */
    public MBlockQueue<T> getBlockQueue(String queueName) {
        NavigableSet<String> queue = db.treeSet("queue", Serializer.STRING).createOrOpen();
        return null;
    }

    /**
     * Map create or open
     * @return
     */
    @Override
    public MMap<K, T> getMap() {
        HTreeMap<String, String> map = db.hashMap("sample")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();
        return null;
    }

    private boolean checkDirectory(String path) {
        //TODO check directory;
        return false;
    }

    class CommitWorker extends Thread {
        private boolean isStart = true;
        public void run() {
            while(isStart) {
                db.commit();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}
            }
        }
        public void terminate() {
            this.isStart = false;
        }
    }

}
