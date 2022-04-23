package net.mapdb.database.queue;

import net.mapdb.database.ManagedStore;

public interface MQueue<T> extends ManagedStore {
    /**
     * 가장 오래된 데이터를 조회 한다.     *
     * 데이터를 삭제 후 반환 한다.
     * @return 데이터가 없는 경우 null반환
     */
    public T poll();
    public long size();

    public void push(T value);
}
