package com.github.horae.exchanger;

import com.github.horae.core.Partition;
import com.github.horae.core.TaskExchanger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by yanghua on 8/25/15.
 */
public class RedisExchanger implements TaskExchanger {

    private static final Log logger = LogFactory.getLog(RedisExchanger.class);

    private Partition partition;

    public RedisExchanger(Partition partition) {
        this.partition = partition;

        //pre-init
        Jedis jedis = null;
        try {
            jedis = RedisConnManager.getJedis(this.partition);
        } finally {
            if (jedis != null) RedisConnManager.returnJedis(this.partition, jedis);
        }
    }

    public void enqueue(String msgStr, String queue) {
        Jedis jedis = null;
        try {
            jedis = RedisConnManager.getJedis(this.partition);
            jedis.lpush(queue, msgStr);
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) RedisConnManager.returnJedis(this.partition, jedis);
        }
    }

    public DequeueObject dequeue(String queue, long timeoutOfSeconds) {
        if (timeoutOfSeconds == -1) {
            timeoutOfSeconds = Integer.MAX_VALUE;
        }

        Jedis jedis = null;
        try {
            jedis = RedisConnManager.getJedis(this.partition);
            List<String> resultList = jedis.brpop((int) timeoutOfSeconds, queue);
            if (resultList != null && resultList.size() == 2) {
                DequeueObject obj = new DequeueObject();
                obj.setId("");
                obj.setBody(resultList.get(1));
                return obj;
            }

            return new DequeueObject();
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) RedisConnManager.returnJedis(this.partition, jedis);
        }
    }

    public void multiEnqueue(String[] msgStrs, String queue) {
        Jedis jedis = null;
        try {
            jedis = RedisConnManager.getJedis(this.partition);
            jedis.lpush(queue, msgStrs);
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) RedisConnManager.returnJedis(this.partition, jedis);
        }
    }

    @Deprecated
    public void ack(String msgId) {
        throw new UnsupportedOperationException("unsupported operation method");
    }
}
