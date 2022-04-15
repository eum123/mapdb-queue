package net.mapdb.database.common.policy;

import org.checkerframework.checker.units.qual.A;

import java.util.Comparator;

public interface CommitPolicy extends Comparator<A> {
    CommitPolicy ASYNC = AsyncCommitPolicy.builder().build();

    @Override
    default int compare(A first, A second) {
        return ((Comparable) first).compareTo(second);
    }


}
