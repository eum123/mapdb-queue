package net.mapdb.database.common.policy;

import lombok.Builder;
import lombok.Getter;

/**
 * 설정된 시간 기준으로 commit을 수행
 * 비동기 commit은 처리 속도 향상됨.
 */
@Builder
public class AsyncCommitPolicy implements CommitPolicy{
    /**
     * Commit을 수행할 주기(millisecond)
     */
    @Getter
    @Builder.Default
    private long interval = 1000;


    @Override
    public boolean equals(Object first) {
        if(first == null) {
            return false;
        }

        if(first instanceof AsyncCommitPolicy) {
            return true;
        }

        return false;
    }
}
