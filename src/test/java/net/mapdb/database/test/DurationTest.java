package net.mapdb.database.test;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DurationTest {
    @Test
    public void durationTest() throws InterruptedException {
      //  LocalDateTime start = LocalDateTime.of(new Date());
        LocalDateTime start = new Date().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        TimeUnit.SECONDS.sleep(1);

        LocalDateTime now = LocalDateTime.now();

        Duration d = Duration.between(start, now);

        System.out.println(d.getSeconds());
    }

    @Test
    public void durationSecondTest() {
        LocalDateTime start = LocalDateTime.of(1990, 12, 1, 1, 1, 1);
        LocalDateTime now = LocalDateTime.now();

        Duration d = Duration.between(start, now);

        System.out.println(d.getSeconds());
    }
}
