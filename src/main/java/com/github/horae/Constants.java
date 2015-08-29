package com.github.horae;

/**
 * Created by yanghua on 8/19/15.
 */
public class Constants {

    public static final String KEY_COMMON_SERVICE_CONFIG_INFO = "common.serviceConfigInfo";
    public static final String KEY_MASTER_BASIC_INFO          = "masterBasicInfo";
    public static final String KEY_LOCK_FOR_MASTER            = "lockForMaster";
    public static final String KEY_HEARTBEAT_CHANNEL_NAME     = "channel.heartbeat.master";
    public static final String KEY_UPSTREAM_CHANNEL_NAME      = "channel.upstream";
    public static final String KEY_DOWNSTREAM_CHANNEL_NAME    = "channel.downstream";
    public static final String KEY_TASK_QUEUE_SET             = "taskQueueSet";
    public static final String KEY_PREFIX_SLAVE_BASIC_INFO    = "slaveBasicInfo.";

    //heartbeat
    public static final String HKEY_HEARTBEAT_CHANNEL_NAME      = "heartbeatChannelName";
    public static final String HKEY_UPSTREAM_CHANNEL_NAME       = "upstreamChannelName";
    public static final String HKEY_DOWNSTREAM_CHANNEL_NAME     = "downstreamChannelName";
    public static final String HKEY_HEARTBEAT_INTERVAL          = "heartbeatInterval";
    public static final String HKEY_HEARTBEAT_TIMEOUT           = "heartbeatTimeout";
    public static final String HKEY_SUBSCRIBE_WAIT_CYCLE_TOTAL  = "subscribeWaitCycleTotal";
    public static final String HKEY_FORWARD_CHECK_CYCLE_TOTAL   = "forwardCheckCycleTotal";
    public static final String HKEY_ARBITRATOR_SLEEP_TIME_RATIO = "arbitratorSleepTimeRatio";

    //service node basic info key
    public static final String HKEY_SERVICE_ID               = "serviceId";
    public static final String HKEY_LAST_MASTER_HB_TIMESTAMP = "lastMasterHBTimestamp";
    public static final String HKEY_STATUS                   = "status";
    public static final String HKEY_START_TIME               = "startTime";
    public static final String HKEY_STOP_TIME                = "stopTime";

    public static final String SERVICE_ID_KEY                  = "serviceId";
    public static final String IS_MASTER_KEY                   = "master";
    public static final String HEARTBEAT_CHANNEL_NAME_KEY      = "heartbeatChannelName";
    public static final String UPSTREAM_CHANNEL_NAME_KEY       = "upstreamChannelName";
    public static final String DOWNSTREAM_CHANNEL_NAME_KEY     = "downstreamChannelName";
    public static final String HEARTBEAT_INTERVAL_KEY          = "heartbeatInterval";
    public static final String HEARTBEAT_TIMEOUT_KEY           = "heartbeatTimeout";
    public static final String SUBSCRIBE_WAIT_CYCLE_TOTAL_KEY  = "subscribeWaitCycleTotal";
    public static final String FORWARD_CHECK_CYCLE_TOTAL_KEY   = "forwardCheckCycleTotal";
    public static final String ARBITRATOR_SLEEP_TIME_RATIO_KEY = "arbitratorSleepTimeRatio";

    //service status
    public static final String SERVICE_STATUS_ONLINE  = "online";
    public static final String SERVICE_STATUS_OFFLINE = "offline";

    public static final int  DEFAULT_MASTER_LOCK_TIMEOUT       = 5;
    public static final long DEFAULT_ENQUEUE_TIMEOUT_OF_MILLIS = 5000;
    public static final long DEQUEUE_NON_TIMEOUT               = -1;

    //redis config
    public static final String REDIS_HOST_KEY = "redis.host";
    public static final String REDIS_PORT_KEY = "redis.port";

    public static final String PARTITION_ROOT_KEY      = "partition.root";
    public static final String PARTITION_MATRIX_SUFFIX = ".matrix";
    public static final String PARTITION_HOST_SUFFIX   = ".host";
    public static final String PARTITION_PORT_SUFFIX   = ".port";
    public static final String PARTITION_CLASS_SUFFIX  = ".class";

    public static final String MATRIX_REDIS  = "redis";
    public static final String MATRIX_DISQUE = "disque";

}
