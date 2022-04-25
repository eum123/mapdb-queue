package net.mapdb.database.queue;

import net.mapdb.database.Database;
import net.mapdb.database.FileDatabase;
import net.mapdb.database.FileDatabaseConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BlockingQueuePerformanceTest {
    Database<String, String> db;
    boolean isFinish = false;
    int COUNT = 10000;

    long start = 0;

    @BeforeEach
    public void init() throws Exception {
        new File("./file/sample.db").deleteOnExit();

        FileDatabaseConfig config = FileDatabaseConfig.builder().filePath("./file").fileName("sample.db").build();
        this.db = new FileDatabase(config);

        db.start();
    }
    @Test
    public void queueTest()  throws Exception {
        MBlockQueue<String> queue = db.getBlockQueue("Q1");

        Executors.newSingleThreadExecutor().submit(() -> {
            while(true) {

                StringBuilder builder = new StringBuilder();
                builder.append("-----------------------------------").append("\n");
                File f = new File("./file/");
                File[] files = f.listFiles();
                for(File ff : files) {
                    builder.append(ff.getName()).append(":").append(ff.length()).append("\n");
                }
                builder.append("-----------------------------------").append("\n");
                System.out.println(builder.toString());

                if(isFinish){
                    break;
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        });

        Executors.newSingleThreadExecutor().submit(() -> {
            int count = 0;
            while(true){

                String value = null;
                try {
                    value = queue.poll();

              //      System.out.println(count + " POLL:"+ value);

                    count++;
                    if(count >= COUNT) {
                        break;
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }

            }
            isFinish = true;

            System.out.println("D:" + (System.currentTimeMillis() - start));
        });

        TimeUnit.SECONDS.sleep(1);

        Executors.newSingleThreadExecutor().submit(() -> {
            start = System.currentTimeMillis();

            for(int i=0 ; i<COUNT ; i++) {
            //    System.out.println("PUSH:"+i);
                queue.push(String.valueOf(i)+"_"+ "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789");

            }
        });

        while(true) {
            if(isFinish){
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }

        Assertions.assertEquals(0, queue.size());
    }



    @AfterEach
    public void destory() throws Exception {
        db.close();

        File f = new File("./file/");
        File[] files = f.listFiles();
        for(File ff : files) {
            ff.deleteOnExit();
        }
    }
}
