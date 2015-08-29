package com.github.horae.core;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yanghua on 8/19/15.
 */
public abstract class TaskTemplate implements Runnable {

    protected CountDownLatch      signal;
    protected String              queueName;
    protected Map<String, Object> configContext;
    protected Partition           partition;
    protected TaskExchanger       taskExchanger;

    public TaskTemplate(CountDownLatch signal,
                        String queueName,
                        Map<String, Object> configContext,
                        Partition partition,
                        TaskExchanger taskExchanger) {
        this.signal = signal;
        this.queueName = queueName;
        this.configContext = configContext;
        this.partition = partition;
        this.taskExchanger = taskExchanger;
    }

}
