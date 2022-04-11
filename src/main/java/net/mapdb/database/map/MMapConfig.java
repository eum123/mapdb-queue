package net.mapdb.database.map;


import lombok.Builder;
import net.mapdb.database.listener.ExpireListener;

@Builder
public class MMapConfig {
    //메시지 expirer가 동작 주기
    private long interval = 1000;

    private ExpireListener listener;

}
