package com.github.horae.task;

import com.github.horae.core.Partition;
import com.github.horae.core.TaskExchanger;
import com.github.horae.core.TaskTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yanghua on 8/26/15.
 */
public class RedisTask extends TaskTemplate {

    private static final Log logger = LogFactory.getLog(RedisTask.class);

    public RedisTask(CountDownLatch signal, String queueName,
                     Map<String, Object> configContext,
                     Partition partition, TaskExchanger taskExchanger) {
        super(signal, queueName, configContext, partition, taskExchanger);
    }

    public void run() {
        try {
            signal.await();
        } catch (InterruptedException e) {

        } finally {

        }
    }

}
