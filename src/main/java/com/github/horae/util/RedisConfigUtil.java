package com.github.horae.util;

import com.github.horae.Constants;
import com.google.common.base.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

/**
 * Created by yanghua on 8/22/15.
 */
public class RedisConfigUtil {

    private static final Log logger = LogFactory.getLog(RedisConfigUtil.class);

    private static volatile JedisPool pool = null;

    public static Optional<Jedis> getJedis(Map<String, Object> configContext) {
        if (pool == null) {
            synchronized (RedisConfigUtil.class) {
                if (pool == null) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxIdle(5);
                    config.setMaxTotal(100);
                    config.setTestOnBorrow(true);
                    try {
                        String host = configContext.get(Constants.REDIS_HOST_KEY).toString();
                        int port = Integer.parseInt(configContext.get(Constants.REDIS_PORT_KEY).toString());
                        pool = new JedisPool(config, host, port);
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            }
        }

        if (pool == null) return Optional.absent();

        Jedis jedis = pool.getResource();
        jedis.select(1);

        return Optional.of(jedis);
    }

    public static void returnResource(Jedis jedis) {
        if (jedis == null || pool == null) {
            throw new RuntimeException("object jedis or pool can not be null. " +
                                           " you should call method :[getPool] to get jedis object ");
        }

        pool.returnResourceObject(jedis);
    }

    public static void destroy() {
        if (pool != null) pool.destroy();
    }

    public static long redisTime(Map<String, Object> configContext) {
        Jedis jedis = null;
        try {
            jedis = getJedis(configContext).get();

            return Long.parseLong(jedis.time().get(0));
        } finally {
            if (jedis != null) returnResource(jedis);
        }
    }

}
