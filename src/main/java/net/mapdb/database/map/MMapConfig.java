package net.mapdb.database.map;


import lombok.Builder;
import lombok.Getter;
import net.mapdb.database.listener.ExpireListener;

@Builder
public class MMapConfig {
    //메시지 expirer 동작 주기
    @Builder.Default
    @Getter
    private long interval = 1000;

    @Getter
    private ExpireListener listener;

}