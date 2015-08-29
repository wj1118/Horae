package com.github.horae.core;

/**
 * Created by yanghua on 8/25/15.
 */
public abstract class CooperateThread implements Runnable {

    public abstract void startup();

    public abstract void shutdown();

}
