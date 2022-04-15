package net.mapdb.database;

import org.junit.jupiter.api.Test;

public class FileDatabaseTest {
    @Test
    public void test() {

        Database db = new FileDatabase<String>(FileDatabaseConfig.builder().fileName("sample").filePath("./")
                .build());
   //     MQueue<String> q = db.getQueue("testQueue");
    }
}
