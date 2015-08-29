package com.github.horae.core;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by yanghua on 8/26/15.
 */
public class Event implements Serializable {

    private String              id;
    private String              type;
    private String              source;
    private Map<String, String> context;

    public Event() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Map<String, String> getContext() {
        return context;
    }

    public void setContext(Map<String, String> context) {
        this.context = context;
    }
}
