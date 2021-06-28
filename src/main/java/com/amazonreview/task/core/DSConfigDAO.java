package com.amazonreview.task.core;

import com.amazonreview.task.exception.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class DSConfigDAO implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DSConfigDAO.class);
    private CassandraCqlDatastore cqlDS;
    public DSConfigDAO(CassandraCqlDatastore cqlDS) {
        this.cqlDS = cqlDS;
    }
    /**
     * Creates connection from connection pool.
     *
     * @throws ConnectionException the connection exception
     */
    public void register() throws ConnectionException {
        try {
            cqlDS.connect();
        } catch (ConnectionException e) {
            logger.error("Failed to Connect Data Store...", e);
            throw e;
        }
    }

    /**
     * Close connection.
     */
    public void unregister() {
        try {
            cqlDS.close();
        } catch (Exception e) {
            logger.error("Failed to Close Data Store...", e);
        }
    }
}
