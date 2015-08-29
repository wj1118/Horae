package com.github.horae.core;

import com.github.horae.Constants;
import com.github.horae.exchanger.DisqueConnManager;
import com.github.horae.exchanger.DisqueExchanger;
import com.github.horae.exchanger.RedisConnManager;
import com.github.horae.exchanger.RedisExchanger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * Created by yanghua on 8/25/15.
 */
public class TaskExchangerManager {

    private static final Log logger = LogFactory.getLog(TaskExchangerManager.class);

    public static TaskExchanger createTaskExchanger(Map<String, Object> configContext,
                                                    Partition partition) {
        if (partition.getMatrix().equals(Constants.MATRIX_REDIS)) {
            return new RedisExchanger(partition);
        } else if (partition.getMatrix().equals(Constants.MATRIX_DISQUE)) {
            return new DisqueExchanger(partition);
        } else {
            logger.error("unsupported matrix : " + partition.getMatrix());
            throw new RuntimeException("unsupported matrix : " + partition.getMatrix());
        }
    }

    public static void destroyTaskExchanger(Map<String, Object> configContext,
                                            Partition partition) {
        if (partition.getMatrix().equals(Constants.MATRIX_REDIS)) {
            RedisConnManager.destroyPool(partition);
        } else if (partition.getMatrix().equals(Constants.MATRIX_DISQUE)) {
            DisqueConnManager.destroyPool(partition);
        } else {
            logger.error("unsupported matrix : " + partition.getMatrix());
            throw new RuntimeException("unsupported matrix : " + partition.getMatrix());
        }
    }

}
