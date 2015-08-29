package com.github.horae.worker;

import com.github.horae.Constants;
import com.github.horae.core.CooperateThread;
import com.github.horae.util.RedisConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by yanghua on 8/19/15.
 */
public class Arbitrator extends CooperateThread {

    private static final Log logger = LogFactory.getLog(Arbitrator.class);

    private Thread currentThread;

    private Map<String, Object> configContext;
    private CountDownLatch      signal;

    private int heartbeatTimeout;
    private int arbitratorSleepTime;

    private int subscribeWaitCycleNum;          //subscribe check num
    private int forwardCheckCycleNum;           //forward check num

    private int subscribeWaitCycleTotal;
    private int forwardCheckCycleTotal;

    public Arbitrator(Map<String, Object> configContext,
                      CountDownLatch signal) {
        this.currentThread = new Thread(this);
        this.currentThread.setName("arbitrator");
        this.configContext = configContext;

        this.subscribeWaitCycleTotal = Integer.parseInt(configContext.get(
            Constants.SUBSCRIBE_WAIT_CYCLE_TOTAL_KEY).toString());
        this.forwardCheckCycleTotal = Integer.parseInt(configContext.get(
            Constants.FORWARD_CHECK_CYCLE_TOTAL_KEY).toString());
        this.heartbeatTimeout = Integer.parseInt(configContext.get(
            Constants.HEARTBEAT_TIMEOUT_KEY).toString());
        int heartbeatInterval = Integer.parseInt(configContext.get(
            Constants.HEARTBEAT_INTERVAL_KEY).toString());
        float astr = Float.parseFloat(configContext.get(
            Constants.ARBITRATOR_SLEEP_TIME_RATIO_KEY).toString());
        this.arbitratorSleepTime = (int) (heartbeatInterval * astr);

        this.signal = signal;
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
        try {
            boolean isMaster = Boolean.parseBoolean(configContext.get(Constants.IS_MASTER_KEY).toString());

            if (!isMaster) {
                while (true) {
                    //double check
                    checkSelfReceivedHeartbeat();
                    checkMasterHeartbeat();

                    boolean lostHeartbeat = lostHeartbeat();

                    if (lostHeartbeat) {
                        try {
                            if (tryLockMaster()) {
                                captureMaster();
                                signal.countDown();
                                break;
                            }
                        } finally {
                            this.unlockMaster();
                        }
                    }

                    TimeUnit.MILLISECONDS.sleep(arbitratorSleepTime);
                }
            }
        } catch (InterruptedException e) {

        }
    }

    private void checkSelfReceivedHeartbeat() {
        boolean isMaster = Boolean.parseBoolean(configContext.get(
            Constants.IS_MASTER_KEY).toString());
        if (!isMaster) {
            final String serviceId = this.configContext.get(Constants.SERVICE_ID_KEY).toString();

            String key = Constants.KEY_PREFIX_SLAVE_BASIC_INFO + serviceId;

            Jedis jedis = null;
            List<Object> results;
            try {
                jedis = RedisConfigUtil.getJedis(this.configContext).get();
                Pipeline pipeline = jedis.pipelined();

                pipeline.time();
                pipeline.hget(key, Constants.HKEY_LAST_MASTER_HB_TIMESTAMP);

                results = pipeline.syncAndReturnAll();
            } finally {
                if (jedis != null) RedisConfigUtil.returnResource(jedis);
            }

            if (results == null) {
                logger.error("can not check last master heartbeat timestamp from service : " + serviceId);
                throw new RuntimeException("can not check last master heartbeat timestamp from service : " + serviceId);
            }

            List<Object> tmpList = (List<Object>) results.get(0);

            long currentTime = Long.parseLong(tmpList.get(0).toString());
            long lmht = Long.parseLong(tmpList.get(1).toString());

            if (currentTime - lmht > this.heartbeatTimeout) {
                this.subscribeWaitCycleNum = --this.subscribeWaitCycleTotal;
            } else {
                //reset to original
                this.subscribeWaitCycleNum = this.subscribeWaitCycleTotal;
            }
        }
    }

    private void checkMasterHeartbeat() {
        boolean isMaster = Boolean.parseBoolean(configContext.get(
            Constants.IS_MASTER_KEY).toString());
        if (!isMaster) {
            String key = Constants.KEY_MASTER_BASIC_INFO;

            Jedis jedis = null;
            List<Object> results;
            try {
                jedis = RedisConfigUtil.getJedis(this.configContext).get();
                Pipeline pipeline = jedis.pipelined();

                pipeline.time();
                pipeline.hget(key, Constants.HKEY_LAST_MASTER_HB_TIMESTAMP);

                results = pipeline.syncAndReturnAll();
            } finally {
                if (jedis != null) RedisConfigUtil.returnResource(jedis);
            }

            if (results == null || results.size() != 2) {
                logger.error("can not check last master heartbeat timestamp from master service ");
                throw new RuntimeException("can not check last master heartbeat timestamp from master service ");
            }

            long currentTime = Long.parseLong(((List) results.get(0)).get(0).toString());
            long lmht = Long.parseLong(results.get(1).toString());

            if (currentTime - lmht > this.heartbeatTimeout) {
                this.forwardCheckCycleNum = --this.forwardCheckCycleTotal;
            } else {
                //reset to original
                this.forwardCheckCycleNum = this.forwardCheckCycleTotal;
            }
        }
    }

    private boolean lostHeartbeat() {
        return this.subscribeWaitCycleNum <= 0 && this.forwardCheckCycleNum <= 0;
    }

    private boolean tryLockMaster() {
        long currentTime = RedisConfigUtil.redisTime(configContext);
        String val = String.valueOf(currentTime + Constants.DEFAULT_MASTER_LOCK_TIMEOUT + 1);
        Jedis jedis = null;
        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();
            boolean locked = jedis.setnx(Constants.KEY_LOCK_FOR_MASTER, val) == 1;
            if (locked) {
                jedis.expire(Constants.KEY_LOCK_FOR_MASTER,
                             Constants.DEFAULT_MASTER_LOCK_TIMEOUT);

                return true;
            }
        } finally {
            if (jedis != null) RedisConfigUtil.returnResource(jedis);
        }

        return false;
    }

    private void unlockMaster() {
        Jedis jedis = null;
        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();
            jedis.del(Constants.KEY_LOCK_FOR_MASTER);
        } finally {
            if (jedis != null) RedisConfigUtil.returnResource(jedis);
        }
    }

    public void captureMaster() {
        long currentTime = RedisConfigUtil.redisTime(configContext);
        Jedis jedis = null;
        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();

            Pipeline pipeline = jedis.pipelined();
            //modify original master info
            String serviceId = configContext.get(Constants.SERVICE_ID_KEY).toString();
            pipeline.hset(Constants.KEY_MASTER_BASIC_INFO,
                          Constants.HKEY_SERVICE_ID,
                          serviceId);
            pipeline.hset(Constants.KEY_MASTER_BASIC_INFO,
                          Constants.HKEY_START_TIME,
                          String.valueOf(currentTime));
            pipeline.hset(Constants.KEY_MASTER_BASIC_INFO,
                          Constants.HKEY_LAST_MASTER_HB_TIMESTAMP,
                          String.valueOf(currentTime));
            pipeline.hdel(Constants.KEY_MASTER_BASIC_INFO,
                          Constants.HKEY_STOP_TIME);

            //remove old service info
            pipeline.del(Constants.KEY_PREFIX_SLAVE_BASIC_INFO + serviceId);

            pipeline.sync();

            configContext.put(Constants.IS_MASTER_KEY, true);
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) RedisConfigUtil.returnResource(jedis);
        }
    }

}
