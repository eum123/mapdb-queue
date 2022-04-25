package net.mapdb.database.map;

import lombok.extern.slf4j.Slf4j;
import net.mapdb.database.Database;
import net.mapdb.database.FileDatabase;
import net.mapdb.database.FileDatabaseConfig;
import net.mapdb.database.listener.ExpireListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MapExpirerTest {
    Database<String, String> db;

    @BeforeEach
    public void init() throws Exception {
        new File("./file/sample.db").deleteOnExit();

        FileDatabaseConfig config = FileDatabaseConfig.builder().filePath("./file").fileName("sample.db").build();
        this.db = new FileDatabase(config);

        db.start();
    }
    @Test
    public void mapTest()  throws Exception {
        AtomicInteger expireCount = new AtomicInteger(0);
        MMapConfig config = MMapConfig.builder().mapName("M1").expirationInterval(1).keyType(String.class).valueType(String.class).listener(new ExpireListener() {
            @Override
            public void onExpiration(Object data) {
                log.info("on expiration :" + data);

                LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong((String)data)), TimeZone.getDefault().toZoneId());
                LocalDateTime now = LocalDateTime.now();

                expireCount.incrementAndGet();
            }
        }).build();

        MMap<String, String> map = db.getMap(config);
        map.put("1", String.valueOf(System.currentTimeMillis()));
        TimeUnit.SECONDS.sleep(1);
        map.put("2", String.valueOf(System.currentTimeMillis()));

        TimeUnit.SECONDS.sleep(5);

        Assertions.assertEquals(0, map.size());
        Assertions.assertEquals(2, expireCount.get());
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
