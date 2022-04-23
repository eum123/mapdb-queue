package net.mapdb.database;

import net.mapdb.database.queue.MQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

public class FileDatabaseTest {
    @Test
    public void test() throws Exception {

        Database<String, String> db = new FileDatabase<String, String>(FileDatabaseConfig.builder().fileName("sample").filePath("./")
                .build());
        db.start();

        MQueue<String> q = db.getQueue("testQueue");
        q.push("my data");

        Assertions.assertEquals(q.size(), 1);

        Assertions.assertEquals("my data", q.poll());

        Assertions.assertEquals(q.size(), 0);
        db.close();
    }

    @BeforeEach
    public void init() {
        new File("./file/sample.db").deleteOnExit();

    }
    @Test
    public void dbFileTest()  throws Exception {
        FileDatabaseConfig config = FileDatabaseConfig.builder().filePath("./file").fileName("sample.db").build();
        Database<String, String> db = new FileDatabase(config);
        try {
            db.start();

            Assertions.assertTrue(new File("./file/sample.db").exists());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.close();
        }

        //remove file

    }
    @AfterEach
    public void destory() {
        new File("./file/sample.db").deleteOnExit();
    }
}
