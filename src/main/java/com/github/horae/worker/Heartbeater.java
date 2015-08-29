package com.github.horae.worker;

import com.github.horae.Constants;
import com.github.horae.core.CooperateThread;
import com.github.horae.util.RedisConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by yanghua on 8/25/15.
 */
public class Heartbeater extends CooperateThread {

    private static final Log logger = LogFactory.getLog(Heartbeater.class);

    private Thread              currentThread;
    private Map<String, Object> configContext;

    private boolean isSubscriberInit = false;

    private HeartbeatSubscriber subscriber;

    public Heartbeater(Map<String, Object> configContext) {
        this.configContext = configContext;
        this.currentThread = new Thread(this);
        this.currentThread.setName("Heartbeater");
    }

    @Override
    public void startup() {
        this.currentThread.start();
    }

    @Override
    public void shutdown() {
        this.currentThread.interrupt();
    }

    public void run() {
        String channel = configContext.get(Constants.HEARTBEAT_CHANNEL_NAME_KEY).toString();
        long lastSendMillis = System.currentTimeMillis();
        int heartbeatInterval = Integer.parseInt(configContext.get(
            Constants.HEARTBEAT_INTERVAL_KEY).toString());
        float arbitratorSleepTimeRatio = Float.parseFloat(configContext.get(
            Constants.ARBITRATOR_SLEEP_TIME_RATIO_KEY).toString());

        Jedis jedis = null;
        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();
            while (true) {

                boolean isMaster = (Boolean) configContext.get(Constants.IS_MASTER_KEY);

                //publish
                if (isMaster) {
                    if (isSubscriberInit && subscriber != null) {
                        //switch from subscriber to publisher
                        subscriber.shutdown();
                    }

                    long now = System.currentTimeMillis();

                    if (now - lastSendMillis > heartbeatInterval) {
                        lastSendMillis = now;

                        jedis.publish(channel, "heartbeat");
                        long currentTime = Long.parseLong(jedis.time().get(0));
                        jedis.hset(Constants.KEY_MASTER_BASIC_INFO,
                                   Constants.HKEY_LAST_MASTER_HB_TIMESTAMP,
                                   String.valueOf(currentTime));

                        TimeUnit.MILLISECONDS.sleep((int) (heartbeatInterval * arbitratorSleepTimeRatio));
                    }
                } else {
                    //subscribe
                    if (!isSubscriberInit) {
                        subscriber = new HeartbeatSubscriber();
                        subscriber.startup();
                        isSubscriberInit = true;
                    }
                }
            }
        } catch (Exception e) {

        } finally {
            if (jedis != null) RedisConfigUtil.returnResource(jedis);
        }
    }

    public class HeartbeatSubscriber implements Runnable {

        private Thread      currentThread;
        private JedisPubSub jedisPubSub;

        public HeartbeatSubscriber() {
            this.currentThread = new Thread(this);
            this.currentThread.setName("heartbeater-subscriber");

            String serviceId = configContext.get(Constants.SERVICE_ID_KEY).toString();
            final String key = Constants.KEY_PREFIX_SLAVE_BASIC_INFO + serviceId;
            final long lastHeartbeatTimestamp = System.currentTimeMillis();
            jedisPubSub = new JedisPubSub() {

                long innerLastHBTimestamp = lastHeartbeatTimestamp;

                @Override
                public void onMessage(String channel, String message) {
                    long now = System.currentTimeMillis();

                    Jedis innerJedis = null;
                    try {
                        innerJedis = RedisConfigUtil.getJedis(configContext).get();
                        innerJedis.hset(key, Constants.HKEY_LAST_MASTER_HB_TIMESTAMP,
                                        innerJedis.time().get(0));
                    } finally {
                        RedisConfigUtil.returnResource(innerJedis);
                    }

                    innerLastHBTimestamp = now;
                }

            };
        }

        public void startup() {
            this.currentThread.start();
        }

        public void shutdown() {
            this.jedisPubSub.unsubscribe(Constants.KEY_HEARTBEAT_CHANNEL_NAME);
            this.currentThread.interrupt();
        }

        public void run() {
            Jedis jedis = null;

            try {
                jedis = RedisConfigUtil.getJedis(configContext).get();

                jedis.subscribe(jedisPubSub, Constants.KEY_HEARTBEAT_CHANNEL_NAME);
            } finally {
                if (jedis != null) RedisConfigUtil.returnResource(jedis);
            }

        }
    }
}
