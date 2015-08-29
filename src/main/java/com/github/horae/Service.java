package com.github.horae;

import com.github.horae.core.LifeCycleManager;
import com.github.horae.core.Partition;
import com.github.horae.util.RedisConfigUtil;
import com.github.horae.worker.Arbitrator;
import com.github.horae.worker.Heartbeater;
import com.github.horae.worker.PartitionInvoker;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yanghua on 8/19/15.
 */
public class Service implements Daemon {

    private static final Log logger = LogFactory.getLog(Service.class);

    private final Map<String, Object> configContext = new ConcurrentHashMap<String, Object>();

    private Arbitrator             arbitrator;
    private Heartbeater            heartbeater;
    private List<PartitionInvoker> partitionInvokers;

    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        String[] args = daemonContext.getArguments();

        for (String arg : args) {
            logger.info("used config file : " + arg);
        }

        logger.info("initing service.");
        LifeCycleManager.loadProperties(configContext, args);
        LifeCycleManager.envInit(configContext);

        heartbeater = new Heartbeater(configContext);
    }

    public void start() throws Exception {
        logger.info("starting service");
        boolean isMaster = Boolean.parseBoolean(configContext.get(
            Constants.IS_MASTER_KEY).toString());

        heartbeater.startup();

        CountDownLatch signal = new CountDownLatch(1);
        List<Partition> partitions = LifeCycleManager.extractPartitions(configContext);
        partitionInvokers = new ArrayList<PartitionInvoker>(partitions.size());
        for (Partition partition : partitions) {
            PartitionInvoker invoker = new PartitionInvoker(partition,
                                                            configContext,
                                                            signal);
            invoker.startup();

            partitionInvokers.add(invoker);
        }

        if (isMaster) {
            signal.countDown();
        } else {
            //arbitrator
            arbitrator = new Arbitrator(configContext, signal);
            arbitrator.startup();
        }
    }

    public void stop() throws Exception {
        logger.info("stopping service");
        heartbeater.shutdown();

        for (PartitionInvoker invoker : partitionInvokers) {
            invoker.shutdown();
        }

        if (arbitrator != null) {
            arbitrator.shutdown();
        }

        LifeCycleManager.offline(configContext);

        RedisConfigUtil.destroy();
    }

    public void destroy() {
        logger.info("destroying service");
        configContext.clear();
    }

    private void localInit(String[] args) throws DaemonInitException, Exception {
        for (String arg : args) {
            logger.info("used config file : " + arg);
        }

        logger.info("initing service.");
        LifeCycleManager.loadProperties(configContext, args);
        LifeCycleManager.envInit(configContext);

        heartbeater = new Heartbeater(configContext);
    }

    //for local test
    public static void main(String[] args) {
        try {
            Service service = new Service();

            String[] configPaths = new String[3];
            configPaths[0] = Service.class.getClassLoader()
                                          .getResource("service.properties").getFile();
            configPaths[1] = Service.class.getClassLoader()
                                          .getResource("redisConf.properties").getFile();
            configPaths[2] = Service.class.getClassLoader()
                                          .getResource("partition.properties").getFile();

            service.localInit(configPaths);
            service.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
