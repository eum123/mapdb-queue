package net.mapdb.database.test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mapdb.*;
import org.mapdb.serializer.GroupSerializer;

import java.util.NavigableSet;
import java.util.concurrent.ConcurrentMap;
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

        NavigableSet<String> queue = db.treeSet("queue", GroupSerializer.STRING).createOrOpen();


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

    @Test
    public void iteratorTest() {
        DB db = DBMaker.fileDB("queue.db").transactionEnable().fileChannelEnable().fileDeleteAfterClose().make();

        HTreeMap<String, Long> map = db.hashMap("index")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.LONG)
                .createOrOpen();

        map.put("INDEX", 0L);

        NavigableSet<String> queue = db.treeSet("queue", GroupSerializer.STRING).createOrOpen();

        for(int i=0;i<100;i++) {
            queue.add(String.valueOf(i));
        }

        db.commit();

        System.out.println("insert size:" + queue.size());

        queue.iterator().forEachRemaining(x -> {
            System.out.println("q :" + x);
        });

        Assertions.assertEquals(100, queue.size());

        db.commit();

        db.close();
    }

    @Test
    public void sequenceTest() {
        DB db = DBMaker.fileDB("queue.db").transactionEnable().fileChannelEnable().fileDeleteAfterClose().make();

        NavigableSet<String> queue = db.treeSet("queue", GroupSerializer.STRING).createOrOpen();

        for(int i=0;i<100;i++) {
            queue.add("H" +String.valueOf(i));
        }
        System.out.println(queue.hashCode());
        db.commit();

        System.out.println("insert size:" + queue.size());

        for(int i=0;i<100;i++) {
            String data = queue.pollFirst();
            System.out.println(i + " queue: " + data);
            Assertions.assertEquals("H" +String.valueOf(i), data);
        }
        Assertions.assertEquals(0, queue.size());

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

        DB db = DBMaker.fileDB("data.db").transactionEnable().fileChannelEnable().fileDeleteAfterClose()
                .make();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(2);


        ConcurrentMap<String, String> map = db.hashMap("data")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .expireAfterUpdate(1, TimeUnit.MINUTES)
                .expireAfterCreate(1, TimeUnit.MINUTES)
                .expireAfterGet(1, TimeUnit.MINUTES)
                .expireExecutorPeriod(1000)
                .expireExecutor(executor)
                .createOrOpen();

        map.put("1", "1");
        TimeUnit.SECONDS.sleep(1);
        map.put("2", "2");
        TimeUnit.SECONDS.sleep(1);
        map.put("3", "3");
        TimeUnit.SECONDS.sleep(1);

        map.values().forEach(x -> System.out.println(x));

        System.out.println(map.size());

        db.close();
    }

    @Test
    public void indexListTest() {
        DB db = DBMaker.fileDB("queue.db").transactionEnable().fileChannelEnable().fileDeleteAfterClose().make();

        IndexTreeList<String> list = db.indexTreeList("index", Serializer.STRING).createOrOpen();



        db.commit();

        db.close();
    }
}
