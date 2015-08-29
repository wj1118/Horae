package com.github.horae.exchanger;

import com.github.horae.core.Partition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yanghua on 8/27/15.
 */
public class RedisConnManager {

    private static final Log logger = LogFactory.getLog(RedisConnManager.class);

    private static volatile Map<String, JedisPool> partitionPoolMap = new ConcurrentHashMap<String, JedisPool>();

    public static Jedis getJedis(Partition partition) {
        if (!partitionPoolMap.containsKey(partition.getName())) {
            synchronized (RedisConnManager.class) {
                if (!partitionPoolMap.containsKey(partition.getName())) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxIdle(5);
                    config.setMaxTotal(20);
                    config.setTestOnBorrow(true);
                    try {
                        String host = partition.getHost();
                        int port = partition.getPort();
                        JedisPool pool = new JedisPool(config, host, port);
                        partitionPoolMap.put(partition.getName(), pool);
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            }
        }

        JedisPool jedisPool = partitionPoolMap.get(partition.getName());
        if (jedisPool == null) {
            logger.error("jedis pool is null. may be concurrency problem.");
            throw new RuntimeException("jedis pool is null. may be concurrency problem.");
        }

        return jedisPool.getResource();
    }

    public static void returnJedis(Partition partition, Jedis jedis) {
        if (!partitionPoolMap.containsKey(partition.getName())) {
            logger.error(" partition with name : " + partition.getName()
                             + " is illegal! or current jedis object do not match this partition. ");
            throw new RuntimeException(" partition with name : " + partition.getName()
                                           + " is illegal! or current jedis object do not match this partition. ");
        }

        JedisPool pool = partitionPoolMap.get(partition.getName());
        pool.returnResourceObject(jedis);
    }

    public static void destroyPool(Partition partition) {
        if (!partitionPoolMap.containsKey(partition.getName())) {
            logger.error(" partition with name : " + partition.getName()
                             + " is illegal! or current jedis object do not match this partition. ");
            throw new RuntimeException(" partition with name : " + partition.getName()
                                           + " is illegal! or current jedis object do not match this partition. ");
        }

        JedisPool pool = partitionPoolMap.get(partition.getName());
        pool.destroy();
    }

}
