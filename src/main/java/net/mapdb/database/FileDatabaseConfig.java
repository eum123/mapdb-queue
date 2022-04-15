package net.mapdb.database;

import lombok.Builder;
import lombok.Getter;
import net.mapdb.database.common.policy.CommitPolicy;

@Builder
public class FileDatabaseConfig {
    @Getter
    private String filePath;
    @Getter
    private String fileName;
    //TODO: commit 동기화 정책 sync, async
    //TODO: async 동기화 인 경우 interval

    @Builder.Default
    @Getter
    private CommitPolicy commitPolicy = CommitPolicy.ASYNC;
}
