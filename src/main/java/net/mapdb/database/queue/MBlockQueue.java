package net.mapdb.database.queue;

import net.mapdb.database.ManagedStore;

import java.util.concurrent.TimeUnit;

public interface MBlockQueue<T> extends ManagedStore {
    /**
     * 가장 오래된 데이터를 조회 한다.
     * 반환할 데이터가 없는 경우 데이터가 추가 될때까지 대기 한다.
     * 데이터를 삭제 후 반환 한다.
     * @return
     */
    T poll() throws InterruptedException;

    /**
     * 가장 오래된 데이터를 조회 한다.
     * 반환할 데이터가 없는 경우 정해진 시간까지 대기하고 데이터를 삭제 후 반환 한다.
     * @param timeout
     * @param unit
     * @return 정해진 시간 이후 데이터가 없는 경우 null 반환.
     * @throws InterruptedException
     */
    T poll(long timeout, TimeUnit unit) throws InterruptedException;
    long size();

    void push(T value);
}
