package com.github.horae.realtime;

import com.github.horae.core.Event;
import com.github.horae.util.EventSerializer;
import redis.clients.jedis.JedisPubSub;

/**
 * Created by yanghua on 8/26/15.
 */
public abstract class DownStreamListener extends JedisPubSub {

    @Override
    @Deprecated
    public void onMessage(String channel, String message) {
        Event event = EventSerializer.deserialize(message);
        onEventMessage(channel, event);
    }

    @Override
    @Deprecated
    public void onPMessage(String pattern, String channel, String message) {
        Event event = EventSerializer.deserialize(message);
        onPEventMessage(pattern, channel, event);
    }

    public abstract void onEventMessage(String channel, Event event);

    public abstract void onPEventMessage(String pattern, String channel, Event event);

}
