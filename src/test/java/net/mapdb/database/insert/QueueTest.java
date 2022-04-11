package net.mapdb.database.insert;

import org.junit.jupiter.api.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.util.NavigableSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class QueueTest {
    /**
     * queue 기능 테스트
     */
    @Test
    public void fifoTest() {
        DB db = DBMaker.fileDB("queue.db").transactionEnable().fileChannelEnable().fileDeleteAfterClose().make();

        HTreeMap<String, Long> map = db.hashMap("index")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.LONG)
                .createOrOpen();

        map.put("INDEX", 0L);

        NavigableSet<String> queue = db.treeSet("queue", Serializer.STRING).createOrOpen();


        db.commit();

        for(int i=0;i<100;i++) {
            queue.add(String.valueOf(i));
        }

        db.commit();

        System.out.println("insert size:" + queue.size());

        for(int i=0;i<100;i++) {
            //처음 데이터를 추출하고  큐에서 삭제한다.
            System.out.println("RCVD:" + queue.pollFirst());


            System.out.println("remain size:" + queue.size());
        }
        db.commit();

        db.close();
    }

    /**
     * expire 기능은 확인 필요
     * @throws Exception
     */
    @Test
    public void expireTest() throws Exception {
        DB dbDisk = DBMaker
                .fileDB("expire.db")
                .make();
        HTreeMap<String, String> exMap = dbDisk.hashMap("ex_data")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();

        DB db = DBMaker.fileDB("data.db").transactionEnable().fileChannelEnable().fileDeleteAfterClose()
                .make();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(2);

        HTreeMap<String, String> map = db.hashMap("data")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .expireAfterCreate(1000, TimeUnit.MILLISECONDS)
                .expireAfterUpdate(1000, TimeUnit.MILLISECONDS)
                .expireExecutor(executor)
                .createOrOpen();

        map.put("1", "1");
        Thread.sleep(3000);

        db.close();
    }
}
