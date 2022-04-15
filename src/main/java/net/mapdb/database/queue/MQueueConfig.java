package net.mapdb.database.queue;

import lombok.Builder;
import lombok.Getter;

@Builder
public class MQueueConfig {
    @Getter
    String queueName;

    @Getter
    private Class valueType;


}
