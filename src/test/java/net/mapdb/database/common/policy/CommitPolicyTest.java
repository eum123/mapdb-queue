package net.mapdb.database.common.policy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CommitPolicyTest {
    @Test
    public void test() {

        AsyncCommitPolicy.builder().build().equals(CommitPolicy.ASYNC);

        Assertions.assertEquals(CommitPolicy.ASYNC, AsyncCommitPolicy.builder().build());
    }
}
