package net.mapdb.database.test;

import lombok.extern.slf4j.Slf4j;
import net.mapdb.database.Database;
import net.mapdb.database.FileDatabase;
import net.mapdb.database.FileDatabaseConfig;
import net.mapdb.database.listener.ExpireListener;
import net.mapdb.database.map.MMap;
import net.mapdb.database.map.MMapConfig;
import net.mapdb.database.queue.MBlockQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceTest {
    Database<String, String> db;

    boolean isStart = true;
    int COUNT = 10000;

    @BeforeEach
    public void init() throws Exception {
        new File("./file/sample.db").deleteOnExit();

        FileDatabaseConfig config = FileDatabaseConfig.builder().filePath("./file").fileName("sample.db").build();
        this.db = new FileDatabase(config);

        db.start();
    }

    @Test
    public void serviceEnvironmentTest() throws Exception {

        MBlockQueue<String> queue = db.getBlockQueue("INCOMING");

        MMapConfig submittingConfig = MMapConfig.builder().mapName("SUBMITTING").expirationInterval(10).keyType(String.class).valueType(String.class).listener(new ExpireListener() {
            @Override
            public void onExpiration(Object data) {
                log.warn("SUBMIT_ACK expiration :" + data);
            }
        }).build();
        MMap<String, String> submitting = db.getMap(submittingConfig);

        MMapConfig reportConfig = MMapConfig.builder().mapName("REPORT").expirationInterval(300).keyType(String.class).valueType(String.class).listener(new ExpireListener() {
            @Override
            public void onExpiration(Object data) {
                log.warn("REPORT expiration :" + data);
            }
        }).build();
        MMap<String, String> report = db.getMap(reportConfig);


        //file logging
        Executors.newSingleThreadExecutor().submit(() -> {
            while(isStart) {

                StringBuilder builder = new StringBuilder();
                builder.append("-----------------------------------").append("\n");
                File f = new File("./file/");
                File[] files = f.listFiles();
                for(File ff : files) {
                    builder.append(ff.getName()).append(":").append(ff.length()).append("\n");
                }
                builder.append("-----------------------------------").append("\n");
                System.out.println(builder.toString());


                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        });

        //incoming to submitting
        Random submittingRate = new Random();
        Executors.newSingleThreadExecutor().submit(() -> {
            int count = 0;
            while(true){

                String value = null;
                try {
                    value = queue.poll();

                    String[] data = value.split("_");

                    TimeUnit.MILLISECONDS.sleep(submittingRate.nextInt(10));

                    submitting.put(data[0], data[1]);

                    log.debug("move to submitting " + data[0]);

                    count ++;
                    if(count > COUNT) {
                        break;
                    }


                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        });

        //submitting to report
        //이동비율 90%
        Random submitAckRate = new Random();
        Executors.newSingleThreadExecutor().submit(() -> {
            int count = 0;
            while(isStart){

                if(submitAckRate.nextInt(10) != 0 ) {
                    String value = submitting.remove(String.valueOf(count));

                    if(value != null) {
                        report.put(String.valueOf(count), value);

                        log.debug("move to report " + count);
                    }  else {
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    log.debug("skip sumbit : " + count);
                }

                count++;
                if(count > COUNT) {
                    break;
                }
            }
        });

        //report
        //결과 수신 비율 80%
        Random reportRate = new Random();
        Executors.newSingleThreadExecutor().submit(() -> {
            int count = 0;
            while(isStart){
                String value = null;
                if(reportRate.nextInt(5) != 0 ) {
                    int limit = reportRate.nextInt(10);
                    for(int i=0 ;i<limit ; i++) {
                        value = report.remove(String.valueOf(count));
                        log.debug("REPORT : RCVD " + value);

                        count++;

                        if(value == null) {
                            break;
                        }
                    }
                } else {
                    log.debug("skip report : " + count);
                }

                if(value == null) {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }



                if(count > COUNT) {
                    isStart = false;
                    break;
                }
            }
        });

        //create
        //
        Random createRate = new Random();
        Executors.newSingleThreadExecutor().submit(() -> {
            int count =0;

            while(true){

                int limit = (createRate.nextInt(10) + 1) * 2;
                for(int i=0 ;i<limit ;i++) {

                    log.debug("create " + count);

                    queue.push(String.valueOf(count) + "_" + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789");

                    count++;

                    if (count > COUNT) {
                        break;
                    }

                }

                if (count > COUNT) {
                    break;
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(createRate.nextInt(10) * 100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });

        while(isStart) {
            TimeUnit.SECONDS.sleep(1);
        }

        log.debug("isStart :" + isStart);
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
