package net.mapdb.database.map;

import net.mapdb.database.Database;
import net.mapdb.database.FileDatabase;
import net.mapdb.database.FileDatabaseConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

public class MapTest {
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

        MMapConfig config = MMapConfig.builder().mapName("M1").keyType(String.class).valueType(String.class).listener(null).build();

        MMap<String, String> map = db.getMap(config);
        map.put("1", "1");
        map.put("2", "2");

        Assertions.assertEquals(2, map.size());

        Assertions.assertEquals("1", map.get("1"));


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
