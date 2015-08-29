package com.github.horae.core;

import java.io.Serializable;

/**
 * Created by yanghua on 8/26/15.
 */
public class Partition implements Serializable {

    private String name;
    private String matrix;
    private String host;
    private int    port;
    private String clazz;

    public Partition() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMatrix() {
        return matrix;
    }

    public void setMatrix(String matrix) {
        this.matrix = matrix;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

}
