package com.github.horae.util;

import com.github.horae.core.Message;
import com.google.gson.Gson;

/**
 * Created by yanghua on 8/20/15.
 */
public class MessageSerializer {

    private static final Gson GSON = new Gson();

    public static String serialize(Message message) {
        return GSON.toJson(message);
    }

    public static Message deserialize(String msgStr) {
        return GSON.fromJson(msgStr, Message.class);
    }

}
