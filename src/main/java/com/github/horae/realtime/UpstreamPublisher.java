package com.github.horae.realtime;

import com.github.horae.Constants;
import com.github.horae.core.Event;
import com.github.horae.util.EventSerializer;
import com.github.horae.util.RedisConfigUtil;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by yanghua on 8/26/15.
 */
public class UpstreamPublisher {

    public static void publish(Event event, Map<String, Object> configContext) {
        String eventStr = EventSerializer.serialize(event);

        Jedis jedis = null;
        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();
            jedis.publish(Constants.KEY_UPSTREAM_CHANNEL_NAME, eventStr);
        } finally {
            if (jedis != null) RedisConfigUtil.returnResource(jedis);
        }
    }

}
