package net.mapdb.database.test;

import net.mapdb.database.Database;
import net.mapdb.database.FileDatabase;
import net.mapdb.database.FileDatabaseConfig;
import net.mapdb.database.queue.MBlockQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class FileSaveTest {
    Database<String, String> db;
    boolean isFinish = false;
    int COUNT = 100;

    @BeforeEach
    public void init() throws Exception {
        open();
    }

    private void open() throws Exception {
        new File("./file/sample.db").deleteOnExit();

        FileDatabaseConfig config = FileDatabaseConfig.builder().filePath("./file").fileName("sample.db").build();
        this.db = new FileDatabase(config);

        db.start();
    }

    @Test
    public void queueTest()  throws Exception {
        MBlockQueue<String> queue1 = db.getBlockQueue("Q1");

        for(int i= 0 ;i<1000;i++) {
            queue1.push(String.valueOf(i));
        }

        Assertions.assertEquals(1000, queue1.size());

        close();

        TimeUnit.SECONDS.sleep(5);

        open();

        MBlockQueue<String> queue2 = db.getBlockQueue("Q1");

        Assertions.assertEquals(1000, queue2.size());
    }



    @AfterEach
    public void destory() throws Exception {
        close();
    }

    private void close() throws Exception {
        db.close();

        File f = new File("./file/");
        File[] files = f.listFiles();
        for(File ff : files) {
            ff.deleteOnExit();
        }
    }
}
