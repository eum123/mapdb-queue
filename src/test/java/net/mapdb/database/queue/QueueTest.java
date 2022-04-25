package net.mapdb.database.queue;

import net.mapdb.database.Database;
import net.mapdb.database.FileDatabase;
import net.mapdb.database.FileDatabaseConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

public class QueueTest {
    Database<String, String> db;

    @BeforeEach
    public void init() throws Exception {
        new File("./file/sample.db").deleteOnExit();

        FileDatabaseConfig config = FileDatabaseConfig.builder().filePath("./file").fileName("sample.db").build();
        this.db = new FileDatabase(config);

        db.start();
    }
    @Test
    public void queueTest()  throws Exception {
        MQueue<String> queue = db.getQueue("Q1");

        Assertions.assertEquals(0, queue.size());

        queue.push("hello");

        Assertions.assertEquals(1, queue.size());

        Assertions.assertEquals("hello", queue.poll());

        Assertions.assertEquals(0, queue.size());
    }

    @Test
    public void iteratorTest() throws Exception {
        MQueue<String> queue = db.getQueue("Q1");


        for(int i=0;i<100;i++) {
            queue.push(String.valueOf(i));
        }
        Assertions.assertEquals(100, queue.size());




        for(int i=0;i<100;i++) {
            Assertions.assertEquals(String.valueOf(i), queue.poll());
            System.out.println("i:" + i);

        }

        Assertions.assertEquals(0, queue.size());

    }

    @AfterEach
    public void destory() throws Exception {
        db.close();
        new File("./file/sample.db").deleteOnExit();
    }
}
