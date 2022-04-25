package net.mapdb.database.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ExecutorTest {

    @Test
    public void executorTest() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(()->{
            while(true) {
                TimeUnit.SECONDS.sleep(1);
            }
        });

        TimeUnit.SECONDS.sleep(5);

        executor.shutdown();

        TimeUnit.SECONDS.sleep(2);

        System.out.println(executor.isTerminated());
        executor.awaitTermination(5, TimeUnit.SECONDS);
        System.out.println(executor.isTerminated());

        executor.shutdownNow();
        TimeUnit.SECONDS.sleep(1);

        System.out.println(executor.isTerminated());
    }
}
