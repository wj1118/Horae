package com.github.horae.exchanger;

import com.github.horae.core.Partition;
import com.github.xetorthio.jedisque.Jedisque;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yanghua on 8/24/15.
 */
public class DisqueConnManager {

    private static final Log logger = LogFactory.getLog(DisqueConnManager.class);

    private static final Map<String, JedisquePool> poolPartitionMap
        = new ConcurrentHashMap<String, JedisquePool>();

    public static Jedisque getJedisque(Partition partition) {
        if (!poolPartitionMap.containsKey(partition.getName())) {
            synchronized (DisqueConnManager.class) {
                if (!poolPartitionMap.containsKey(partition.getName())) {
                    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
                    config.setMaxTotal(20);
                    config.setMaxIdle(5);
                    config.setTestOnBorrow(true);

                    JedisquePool jedisquePool = new JedisquePool(config,
                                                                 new JedisqueFactory(partition));
                    poolPartitionMap.put(partition.getName(), jedisquePool);
                }
            }
        }

        JedisquePool pool = poolPartitionMap.get(partition.getName());
        if (pool == null) {
            logger.error("jedisque pool is null. may be concurrency problem.");
            throw new RuntimeException("jedisque pool is null. may be concurrency problem.");
        }

        return pool.getResource();
    }

    public static void returnJedisque(Partition partition, Jedisque jedisque) {
        if (!poolPartitionMap.containsKey(partition.getName())) {
            logger.error(" partition with name : " + partition.getName()
                             + " is illegal! or current jedis object do not match this partition. ");
            throw new RuntimeException(" partition with name : " + partition.getName()
                                           + " is illegal! or current jedis object do not match this partition. ");
        }

        JedisquePool pool = poolPartitionMap.get(partition.getName());
        pool.returnResourceObject(jedisque);
    }

    public static void destroyPool(Partition partition) {
        if (!poolPartitionMap.containsKey(partition.getName())) {
            logger.error(" partition with name : " + partition.getName()
                             + " is illegal! or current jedis object do not match this partition. ");
            throw new RuntimeException(" partition with name : " + partition.getName()
                                           + " is illegal! or current jedis object do not match this partition. ");
        }

        JedisquePool pool = poolPartitionMap.get(partition.getName());
        pool.destroy();
    }

    public abstract static class AbstractPool<T> {
        private static final Log logger = LogFactory.getLog(AbstractPool.class);

        protected GenericObjectPool<T> internalPool;

        public AbstractPool(final GenericObjectPoolConfig poolConfig,
                            PooledObjectFactory<T> factory) {
            this.initPool(poolConfig, factory);
        }

        private void initPool(final GenericObjectPoolConfig poolConfig,
                              PooledObjectFactory<T> factory) {
            if (this.internalPool != null) {
                closeInternalPool();
            }

            this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
        }

        public T getResource() {
            try {
                return internalPool.borrowObject();
            } catch (Exception e) {
                logger.error(e);
                throw new RuntimeException("can not get a resource from the pool ", e);
            }
        }

        public void returnResourceObject(final T resource) {
            internalPool.returnObject(resource);
        }

        public void returnBrokenResource(final T resource) {
            returnBrokenResourceObject(resource);
        }

        public void returnResource(final T resource) {
            returnResourceObject(resource);
        }

        public void destroy() {
            closeInternalPool();
        }

        protected void returnBrokenResourceObject(final T resource) {
            try {
                internalPool.invalidateObject(resource);
            } catch (Exception e) {
                throw new RuntimeException("Could not return the resource to the pool", e);
            }
        }

        protected void closeInternalPool() {
            internalPool.close();
        }
    }

    public static class JedisqueFactory implements PooledObjectFactory<Jedisque> {

        private static final String DISQUE_URI_PATTERN = "disque://%s:%s";

        private Partition partition;

        public JedisqueFactory(Partition partition) {
            this.partition = partition;

        }

        public PooledObject<Jedisque> makeObject() throws Exception {
            String uriStr = String.format(DISQUE_URI_PATTERN,
                                          partition.getHost(),
                                          partition.getPort());

            URI disqueUri = null;
            try {
                disqueUri = new URI(uriStr);
            } catch (URISyntaxException e) {
                logger.error(e);
                throw new RuntimeException(e);
            }
            Jedisque jedisque = new Jedisque(disqueUri);

            return new DefaultPooledObject<Jedisque>(jedisque);
        }

        public void destroyObject(PooledObject<Jedisque> pooledObject) throws Exception {
            Jedisque jedisque = pooledObject.getObject();
            if (jedisque != null && jedisque.isConnected()) {
                jedisque.close();
            }
        }

        public boolean validateObject(PooledObject<Jedisque> pooledObject) {
            return true;
        }

        public void activateObject(PooledObject<Jedisque> pooledObject) throws Exception {

        }

        public void passivateObject(PooledObject<Jedisque> pooledObject) throws Exception {

        }
    }

    public static class JedisquePool extends AbstractPool<Jedisque> {

        public JedisquePool(GenericObjectPoolConfig poolConfig, PooledObjectFactory<Jedisque> factory) {
            super(poolConfig, factory);
        }

    }

}
