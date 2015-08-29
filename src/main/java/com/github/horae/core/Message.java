package com.github.horae.core;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by yanghua on 8/20/15.
 */
public class Message implements Serializable {

    private String              id;
    private long                commitTime;
    private Map<String, Object> msgContext;

    public Message() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getCommitTime() {
        return commitTime;
    }

    public void setCommitTime(long commitTime) {
        this.commitTime = commitTime;
    }

    public Map<String, Object> getMsgContext() {
        return msgContext;
    }

    public void setMsgContext(Map<String, Object> msgContext) {
        this.msgContext = msgContext;
    }
}
