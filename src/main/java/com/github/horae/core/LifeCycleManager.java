package com.github.horae.core;

import com.github.horae.Constants;
import com.github.horae.util.RedisConfigUtil;
import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by yanghua on 8/19/15.
 */
public class LifeCycleManager {

    private static final Log logger = LogFactory.getLog(LifeCycleManager.class);

    public static void loadProperties(Map<String, Object> configContext, String[] configPaths) {
        try {
            for (String configPath : configPaths) {
                Properties properties = new Properties();
                properties.load(new FileReader(configPath));

                Enumeration<?> enumeration = properties.propertyNames();
                while (enumeration.hasMoreElements()) {
                    String key = enumeration.nextElement().toString();
                    configContext.put(key, properties.get(key));
                }
            }
        } catch (IOException e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }

    public static void envInit(Map<String, Object> configContext) {
        Jedis jedis = null;

        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();

            boolean isMaster = Boolean.parseBoolean(configContext.get(Constants.IS_MASTER_KEY).toString());
            if (isMaster) {
                logger.info(" current service is master. ");
                doEnvInitForMaster(configContext, jedis);
            } else {
                logger.info(" current service is slave. ");
                doEnvInitForSlave(configContext, jedis);
            }
        } finally {
            if (jedis != null) {
                RedisConfigUtil.returnResource(jedis);
            }
        }
    }

    public static void clearCoreData(Map<String, Object> configContext) {
        //clear redis core data
        Jedis jedis = null;
        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();
            jedis.del(Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
        } finally {
            RedisConfigUtil.returnResource(jedis);
        }
    }

    public static List<Partition> extractPartitions(Map<String, Object> configContext) {
        String[] partitionPrefixes = configContext.get(Constants.PARTITION_ROOT_KEY).toString().split(",");

        if (partitionPrefixes.length == 0) {
            logger.error("there is not any matrix could be find!");
            throw new RuntimeException("there is not any matrix could be find!");
        }

        List<Partition> partitions = new ArrayList<Partition>(partitionPrefixes.length);
        for (String p : partitionPrefixes) {
            Partition partition = new Partition();
            partition.setName(p);
            partition.setMatrix(configContext.get(p + Constants.PARTITION_MATRIX_SUFFIX).toString());
            partition.setHost(configContext.get(p + Constants.PARTITION_HOST_SUFFIX).toString());
            String portStr = configContext.get(p + Constants.PARTITION_PORT_SUFFIX).toString();
            partition.setPort(Integer.parseInt(portStr));
            partition.setClazz(configContext.get(p + Constants.PARTITION_CLASS_SUFFIX).toString());

            partitions.add(partition);
        }

        return partitions;
    }

    public static List<String> queryQueueKeyList(Map<String, Object> configContext,
                                                 Partition partition) {
        String key = partition.getName() + "." + Constants.KEY_TASK_QUEUE_SET;
        Jedis jedis = null;
        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();
            Set<String> queueKeySet = jedis.smembers(key);

            List<String> queueKeyList = new ArrayList<String>(queueKeySet.size());
            queueKeyList.addAll(queueKeySet);

            return queueKeyList;
        } finally {
            if (jedis != null) RedisConfigUtil.returnResource(jedis);
        }
    }

    public static void offline(Map<String, Object> configContext) {
        boolean isMaster = Boolean.parseBoolean(configContext.get(
            Constants.IS_MASTER_KEY).toString());

        Jedis jedis = null;

        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();

            if (isMaster) {
                jedis.hset(Constants.KEY_MASTER_BASIC_INFO,
                           Constants.HKEY_STATUS,
                           Constants.SERVICE_STATUS_OFFLINE);
                jedis.hset(Constants.KEY_MASTER_BASIC_INFO,
                           Constants.HKEY_STOP_TIME,
                           String.valueOf(RedisConfigUtil.redisTime(configContext)));
            } else {
                String serviceId = configContext.get(Constants.SERVICE_ID_KEY).toString();
                String key = Constants.KEY_PREFIX_SLAVE_BASIC_INFO + serviceId;

                jedis.hset(key, Constants.HKEY_STATUS,
                           Constants.SERVICE_STATUS_OFFLINE);
                jedis.hset(key,
                           Constants.HKEY_STOP_TIME,
                           String.valueOf(RedisConfigUtil.redisTime(configContext)));
            }
        } finally {
            if (jedis != null) RedisConfigUtil.returnResource(jedis);
        }

    }

    private static void doEnvInitForMaster(Map<String, Object> configContext, Jedis jedis) {
        boolean existsCommonConfigKey = jedis.exists(Constants.KEY_COMMON_SERVICE_CONFIG_INFO);

        //if exists core config info, then remove it
        if (existsCommonConfigKey) {
            clearCoreData(configContext);
        }

        //if exists master info before, delete first
        if (jedis.exists(Constants.KEY_MASTER_BASIC_INFO)) {
            jedis.del(Constants.KEY_MASTER_BASIC_INFO);
        }

        initCoreConfigToRedis(jedis, configContext);

        //init service info for master
        initMasterBasicInfo(configContext, jedis);
    }

    private static void doEnvInitForSlave(Map<String, Object> configContext, Jedis jedis) {
        boolean existsCommonConfigKey = jedis.exists(Constants.KEY_COMMON_SERVICE_CONFIG_INFO);

        if (!existsCommonConfigKey) {
            logger.error("current service is slave but can not find redis config key : "
                             + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
            throw new RuntimeException("current service is slave but can not find redis config key : "
                                           + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
        } else {
            dumpConfigInfoFromRedis(jedis, configContext);
        }

        boolean existsMasterBasicInfoKey = jedis.exists(Constants.KEY_MASTER_BASIC_INFO);

        if (!existsMasterBasicInfoKey) {
            logger.error("current service is slave but can not find master's config info!");
            throw new RuntimeException("current service is slave but can not find master's config info!");
        }

        initSlaveBasicInfo(configContext, jedis);
    }

    private static void dumpConfigInfoFromRedis(Jedis jedis, Map<String, Object> configContext) {
        //put into config context
        String heartbeatChannel = jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                             Constants.HKEY_HEARTBEAT_CHANNEL_NAME);
        String upstreamChannel = jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                            Constants.HKEY_UPSTREAM_CHANNEL_NAME);
        String downstreamChannel = jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                              Constants.HKEY_DOWNSTREAM_CHANNEL_NAME);
        String intervalStr = jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                        Constants.HKEY_HEARTBEAT_INTERVAL);
        String timeoutStr = jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                       Constants.HKEY_HEARTBEAT_TIMEOUT);
        String subWaitCycleTotalStr = jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                                 Constants.HKEY_SUBSCRIBE_WAIT_CYCLE_TOTAL);
        String forwardCheckCycleTotal = jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                                   Constants.HKEY_FORWARD_CHECK_CYCLE_TOTAL);
        String astrStr = jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                    Constants.HKEY_ARBITRATOR_SLEEP_TIME_RATIO);

        if (Strings.isNullOrEmpty(heartbeatChannel)) {
            heartbeatChannel = Constants.KEY_HEARTBEAT_CHANNEL_NAME;
        }

        configContext.put(Constants.HEARTBEAT_CHANNEL_NAME_KEY, heartbeatChannel);
        configContext.put(Constants.UPSTREAM_CHANNEL_NAME_KEY, upstreamChannel);
        configContext.put(Constants.DOWNSTREAM_CHANNEL_NAME_KEY, downstreamChannel);

        if (Strings.isNullOrEmpty(intervalStr)) {
            logger.error("can not find hkey : " + Constants.HKEY_HEARTBEAT_INTERVAL
                             + " in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
            throw new RuntimeException("can not find hkey : " + Constants.HKEY_HEARTBEAT_INTERVAL
                                           + "in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
        }

        if (Strings.isNullOrEmpty(timeoutStr)) {
            logger.error("can not find hkey : " + Constants.HKEY_HEARTBEAT_TIMEOUT
                             + " in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
            throw new RuntimeException("can not find hkey : " + Constants.HKEY_HEARTBEAT_TIMEOUT
                                           + " in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
        }

        if (Strings.isNullOrEmpty(subWaitCycleTotalStr)) {
            logger.error("can not find hkey : " + Constants.HKEY_SUBSCRIBE_WAIT_CYCLE_TOTAL
                             + " in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
            throw new RuntimeException("can not find hkey : " + Constants.HKEY_SUBSCRIBE_WAIT_CYCLE_TOTAL
                                           + " in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
        }

        if (Strings.isNullOrEmpty(forwardCheckCycleTotal)) {
            logger.error("can not find hkey : " + Constants.HKEY_FORWARD_CHECK_CYCLE_TOTAL
                             + " in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
            throw new RuntimeException("can not find hkey : " + Constants.HKEY_FORWARD_CHECK_CYCLE_TOTAL
                                           + " in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
        }

        if (Strings.isNullOrEmpty(astrStr)) {
            logger.error("can not find hkey : " + Constants.HKEY_ARBITRATOR_SLEEP_TIME_RATIO
                             + " in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
            throw new RuntimeException("can not find hkey : " + Constants.HKEY_ARBITRATOR_SLEEP_TIME_RATIO
                                           + " in key : " + Constants.KEY_COMMON_SERVICE_CONFIG_INFO);
        }

        configContext.put(Constants.HEARTBEAT_INTERVAL_KEY, Integer.parseInt(intervalStr));
        configContext.put(Constants.HEARTBEAT_TIMEOUT_KEY, Integer.parseInt(timeoutStr));
        configContext.put(Constants.SUBSCRIBE_WAIT_CYCLE_TOTAL_KEY, Integer.parseInt(subWaitCycleTotalStr));
        configContext.put(Constants.FORWARD_CHECK_CYCLE_TOTAL_KEY, Integer.parseInt(forwardCheckCycleTotal));
        configContext.put(Constants.ARBITRATOR_SLEEP_TIME_RATIO_KEY, Float.parseFloat(astrStr));

        dumpPartitionConfigFromRedis(jedis, configContext);
    }

    private static void initCoreConfigToRedis(Jedis jedis, Map<String, Object> configContext) {
        jedis.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                   Constants.HKEY_HEARTBEAT_CHANNEL_NAME,
                   Constants.KEY_HEARTBEAT_CHANNEL_NAME);

        jedis.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                   Constants.HKEY_UPSTREAM_CHANNEL_NAME,
                   Constants.KEY_UPSTREAM_CHANNEL_NAME);

        jedis.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                   Constants.HKEY_DOWNSTREAM_CHANNEL_NAME,
                   Constants.KEY_DOWNSTREAM_CHANNEL_NAME);

        final String heartbeatIntervalStr = configContext.get("heartbeatInterval").toString();
        if (Strings.isNullOrEmpty(heartbeatIntervalStr)) {
            logger.error("can not find key : " + Constants.HKEY_HEARTBEAT_INTERVAL);
            throw new RuntimeException("can not find key : " + Constants.HKEY_HEARTBEAT_INTERVAL);
        }

        final String heartbeatTimeoutStr = configContext.get("heartbeatTimeout").toString();
        if (Strings.isNullOrEmpty(heartbeatTimeoutStr)) {
            logger.error("can not find key : " + Constants.HKEY_HEARTBEAT_TIMEOUT);
            throw new RuntimeException("can not find key : " + Constants.HKEY_HEARTBEAT_TIMEOUT);
        }

        final String subWaitCycleTotalStr = configContext.get("subscribeWaitCycleTotal").toString();
        if (Strings.isNullOrEmpty(subWaitCycleTotalStr)) {
            logger.error("can not find key : " + Constants.HKEY_SUBSCRIBE_WAIT_CYCLE_TOTAL);
            throw new RuntimeException("can not find key : " + Constants.HKEY_SUBSCRIBE_WAIT_CYCLE_TOTAL);
        }

        final String forwardCheckCycleTotalStr = configContext.get("forwardCheckCycleTotal").toString();
        if (Strings.isNullOrEmpty(forwardCheckCycleTotalStr)) {
            logger.error("can not find key : " + Constants.HKEY_FORWARD_CHECK_CYCLE_TOTAL);
            throw new RuntimeException("can not find key : " + Constants.HKEY_FORWARD_CHECK_CYCLE_TOTAL);
        }

        final String astrStr = configContext.get("arbitratorSleepTimeRatio").toString();
        if (Strings.isNullOrEmpty(astrStr)) {
            logger.error("can not find key : " + Constants.HKEY_ARBITRATOR_SLEEP_TIME_RATIO);
            throw new RuntimeException("can not find key : " + Constants.HKEY_ARBITRATOR_SLEEP_TIME_RATIO);
        }

        Pipeline pipeline = jedis.pipelined();
        pipeline.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                      Constants.HKEY_HEARTBEAT_CHANNEL_NAME,
                      Constants.KEY_HEARTBEAT_CHANNEL_NAME);

        pipeline.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                      Constants.HKEY_HEARTBEAT_INTERVAL,
                      heartbeatIntervalStr);

        pipeline.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                      Constants.HKEY_HEARTBEAT_TIMEOUT,
                      heartbeatTimeoutStr);

        pipeline.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                      Constants.HKEY_SUBSCRIBE_WAIT_CYCLE_TOTAL,
                      subWaitCycleTotalStr);

        pipeline.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                      Constants.HKEY_FORWARD_CHECK_CYCLE_TOTAL,
                      forwardCheckCycleTotalStr);

        pipeline.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                      Constants.HKEY_ARBITRATOR_SLEEP_TIME_RATIO,
                      astrStr);

        pipeline.sync();

        //store into config context
        configContext.put(Constants.HEARTBEAT_CHANNEL_NAME_KEY, Constants.KEY_HEARTBEAT_CHANNEL_NAME);
        configContext.put(Constants.UPSTREAM_CHANNEL_NAME_KEY, Constants.KEY_UPSTREAM_CHANNEL_NAME);
        configContext.put(Constants.DOWNSTREAM_CHANNEL_NAME_KEY, Constants.KEY_DOWNSTREAM_CHANNEL_NAME);

        initPartitionConfigToRedis(jedis, configContext);
    }

    private static void initPartitionConfigToRedis(Jedis jedis, Map<String, Object> configContext) {
        String[] partitionPrefixes = configContext.get(Constants.PARTITION_ROOT_KEY).toString().split(",");

        if (partitionPrefixes.length == 0) {
            logger.error("there is not any matrix could be find!");
            throw new RuntimeException("there is not any matrix could be find!");
        }

        jedis.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                   Constants.PARTITION_ROOT_KEY,
                   configContext.get(Constants.PARTITION_ROOT_KEY).toString());

        for (String p : partitionPrefixes) {
            jedis.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                       p + Constants.PARTITION_MATRIX_SUFFIX,
                       configContext.get(p + Constants.PARTITION_MATRIX_SUFFIX).toString());
            jedis.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                       p + Constants.PARTITION_HOST_SUFFIX,
                       configContext.get(p + Constants.PARTITION_HOST_SUFFIX).toString());
            jedis.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                       p + Constants.PARTITION_PORT_SUFFIX,
                       configContext.get(p + Constants.PARTITION_PORT_SUFFIX).toString());
            jedis.hset(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                       p + Constants.PARTITION_CLASS_SUFFIX,
                       configContext.get(p + Constants.PARTITION_CLASS_SUFFIX).toString());
        }
    }

    private static void dumpPartitionConfigFromRedis(Jedis jedis, Map<String, Object> configContext) {
        String partitionRoot = jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                          Constants.PARTITION_ROOT_KEY);

        if (Strings.isNullOrEmpty(partitionRoot)) {
            logger.error("can not find key : " + Constants.PARTITION_ROOT_KEY);
            throw new RuntimeException("can not find key : " + Constants.PARTITION_ROOT_KEY);
        }

        String[] partitionPrefixes = partitionRoot.split(",");

        if (partitionPrefixes.length == 0) {
            logger.error("there is not any matrix could be find!");
            throw new RuntimeException("there is not any matrix could be find!");
        }

        for (String p : partitionPrefixes) {
            configContext.put(p + Constants.PARTITION_MATRIX_SUFFIX,
                              jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                         p + Constants.PARTITION_MATRIX_SUFFIX));
            configContext.put(p + Constants.PARTITION_HOST_SUFFIX,
                              jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                         p + Constants.PARTITION_HOST_SUFFIX));
            configContext.put(p + Constants.PARTITION_PORT_SUFFIX,
                              jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                         p + Constants.PARTITION_PORT_SUFFIX));
            configContext.put(p + Constants.PARTITION_CLASS_SUFFIX,
                              jedis.hget(Constants.KEY_COMMON_SERVICE_CONFIG_INFO,
                                         p + Constants.PARTITION_CLASS_SUFFIX));
        }
    }

    private static void initMasterBasicInfo(Map<String, Object> configContext, Jedis jedis) {
        String serviceId = UUID.randomUUID().toString();
        logger.info("current service identifier : " + serviceId);

        jedis.hset(Constants.KEY_MASTER_BASIC_INFO,
                   Constants.HKEY_SERVICE_ID,
                   serviceId);
        jedis.hset(Constants.KEY_MASTER_BASIC_INFO,
                   Constants.HKEY_STATUS,
                   Constants.SERVICE_STATUS_ONLINE);
        jedis.hset(Constants.KEY_MASTER_BASIC_INFO,
                   Constants.HKEY_START_TIME,
                   String.valueOf(RedisConfigUtil.redisTime(configContext)));

        configContext.put(Constants.SERVICE_ID_KEY, serviceId);
        configContext.put(Constants.IS_MASTER_KEY, true);
    }

    private static void initSlaveBasicInfo(Map<String, Object> configContext, Jedis jedis) {
        String serviceId = UUID.randomUUID().toString();

        logger.info("current service identifier : " + serviceId);

        String key = Constants.KEY_PREFIX_SLAVE_BASIC_INFO + serviceId;

        jedis.hset(key, Constants.HKEY_SERVICE_ID, serviceId);
        jedis.hset(key, Constants.HKEY_STATUS, Constants.SERVICE_STATUS_ONLINE);
        jedis.hset(key, Constants.HKEY_START_TIME,
                   String.valueOf(RedisConfigUtil.redisTime(configContext)));

        configContext.put(Constants.SERVICE_ID_KEY, serviceId);
        configContext.put(Constants.IS_MASTER_KEY, false);
    }

}
