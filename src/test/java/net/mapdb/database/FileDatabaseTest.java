package net.mapdb.database;

import net.mapdb.database.queue.MQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}
