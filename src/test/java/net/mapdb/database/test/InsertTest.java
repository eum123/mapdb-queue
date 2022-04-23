package net.mapdb.database.test;

import org.junit.jupiter.api.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;

public class InsertTest {
    @Test
    public void insertTest() {
        //테스트용 close 후 파일 삭제
        DB db = DBMaker.fileDB("mapdb.db").transactionEnable().fileChannelEnable().fileDeleteAfterClose().make();
        HTreeMap<String, String> map = db.hashMap("sample")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();

        map.clear();
        db.commit();

        File f = new File("mapdb.db");
        System.out.println("file size : " + f.length());

        System.out.println("before size:" + map.size());

        long currentTime = System.currentTimeMillis();
        for(int i = 0; i < 100; i++){
            //1k insert
            map.put("key" + i, "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789");
            //건별 commit 은 속도가 느리다
            //db.commit();
        }

        db.commit();
        System.out.println("after size:" + map.size());

        File f1 = new File("mapdb.db");
        System.out.println("file size : " + f1.length());

        System.out.println("time : " + (System.currentTimeMillis() - currentTime));

        map.clear();
        db.close();


    }

    /**
     * 파일 삭제하지 않고 저장된 내용이 존재하는지 확인
     */
    @Test
    public void reloadTest() {
        DB db = DBMaker.fileDB("mapdb.db").transactionEnable().fileChannelEnable().make();
        HTreeMap<String, String> map = db.hashMap("sample")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();

        map.put("key1", "value1");
        db.commit();
        System.out.println("size:" + map.size());

        db.close();

        DB db1 = DBMaker.fileDB("mapdb.db").transactionEnable().fileChannelEnable().make();
        HTreeMap<String, String> map1 = db1.hashMap("sample")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();

        System.out.println("size : " + map1.size());

        map1.clear();
        db.close();
    }

}
