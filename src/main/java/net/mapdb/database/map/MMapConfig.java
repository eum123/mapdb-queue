package net.mapdb.database.map;


import lombok.Builder;
import lombok.Getter;
import net.mapdb.database.listener.ExpireListener;

@Builder
public class MMapConfig {

    @Getter
    String mapName;

    @Getter
    private Class keyType;

    @Getter
    private Class valueType;

    /**
     * 만료 시간 설정 (second)
     */
    @Builder.Default
    @Getter
    private long expirationInterval = 3600;

    @Getter
    private ExpireListener listener;

}
