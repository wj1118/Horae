package com.github.horae.worker;

import com.github.horae.core.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yanghua on 8/26/15.
 */
public class PartitionInvoker {

    private static final Log logger = LogFactory.getLog(PartitionInvoker.class);

    private Map<String, Object> configContext;
    private Partition           partition;
    private CountDownLatch      signal;
    private ExecutorService     invoker;

    public PartitionInvoker(Partition partition,
                            Map<String, Object> configContext,
                            CountDownLatch signal) {
        this.partition = partition;
        this.configContext = configContext;
        this.signal = signal;
    }

    public void startup() {
        invoker = Executors.newCachedThreadPool();

        List<String> queues = LifeCycleManager.queryQueueKeyList(configContext, partition);
        TaskExchanger taskExchanger = TaskExchangerManager.createTaskExchanger(configContext, partition);

        try {
            Class aClass = Class.forName(partition.getClazz());
            Constructor constructor = aClass.getDeclaredConstructor(CountDownLatch.class,
                                                                    String.class,
                                                                    Map.class,
                                                                    Partition.class,
                                                                    TaskExchanger.class);

            //init task threads
            for (String queue : queues) {
                Object instance = constructor.newInstance(signal, queue, configContext, partition, taskExchanger);
                invoker.submit((TaskTemplate) instance);
            }
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        invoker.shutdownNow();
        TaskExchangerManager.destroyTaskExchanger(configContext, partition);
    }

}
