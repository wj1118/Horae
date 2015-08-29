package com.github.horae.exchanger;

import com.github.horae.Constants;
import com.github.horae.core.Partition;
import com.github.horae.core.TaskExchanger;
import com.github.xetorthio.jedisque.Jedisque;
import com.github.xetorthio.jedisque.Job;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Created by yanghua on 8/25/15.
 */
public class DisqueExchanger implements TaskExchanger {

    private static final Log logger = LogFactory.getLog(DisqueExchanger.class);

    private Partition partition;

    public DisqueExchanger(Partition partition) {
        this.partition = partition;

        //pre-init
        Jedisque jedisque = null;
        try {
            jedisque = DisqueConnManager.getJedisque(partition);
        } finally {
            if (jedisque != null) DisqueConnManager.returnJedisque(partition, jedisque);
        }
    }

    public void enqueue(String msgStr, String queue) {
        Jedisque jedisque = null;

        try {
            jedisque = DisqueConnManager.getJedisque(partition);
            jedisque.addJob(queue, msgStr,
                            Constants.DEFAULT_ENQUEUE_TIMEOUT_OF_MILLIS);
        } finally {
            if (jedisque != null) DisqueConnManager.returnJedisque(partition, jedisque);
        }
    }

    public void multiEnqueue(String[] msgStrs, String queue) {
        Jedisque jedisque = null;

        try {
            jedisque = DisqueConnManager.getJedisque(partition);
            for (String requestStr : msgStrs) {
                jedisque.addJob(queue, requestStr,
                                Constants.DEFAULT_ENQUEUE_TIMEOUT_OF_MILLIS);
            }
        } finally {
            if (jedisque != null) DisqueConnManager.returnJedisque(partition, jedisque);
        }
    }

    public DequeueObject dequeue(String queue, long timeoutOfSeconds) {
        Jedisque jedisque = null;

        try {
            jedisque = DisqueConnManager.getJedisque(partition);
            List<Job> jobs = jedisque.getJob(Long.MAX_VALUE, 1, queue);
            if (jobs == null || jobs.size() == 0) {
                return new DequeueObject();
            }

            DequeueObject obj = new DequeueObject();
            obj.setId(jobs.get(0).getId());
            obj.setBody(jobs.get(0).getStringBody());

            return obj;
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException(e);
        } finally {
            if (jedisque != null) DisqueConnManager.returnJedisque(partition, jedisque);
        }
    }

    public void ack(String id) {
        Jedisque jedisque = null;

        try {
            jedisque = DisqueConnManager.getJedisque(partition);
            jedisque.fastack(id);
        } finally {
            if (jedisque != null) DisqueConnManager.returnJedisque(partition, jedisque);
        }
    }
}
