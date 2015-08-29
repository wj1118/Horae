package com.github.horae.util;

import com.github.horae.core.Event;
import com.google.gson.Gson;

/**
 * Created by yanghua on 8/26/15.
 */
public class EventSerializer {

    private static final Gson GSON = new Gson();

    public static String serialize(Event event) {
        return GSON.toJson(event);
    }

    public static Event deserialize(String eventStr) {
        return GSON.fromJson(eventStr, Event.class);
    }

}
