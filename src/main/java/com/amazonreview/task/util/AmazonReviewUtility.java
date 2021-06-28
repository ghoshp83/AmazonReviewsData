package com.amazonreview.task.util;

import com.amazonreview.task.error.AmazonReviewError;
import com.amazonreview.task.exception.ConnectionException;
import com.amazonreview.task.AmazonReviewTaskDataGenerator;
import com.amazonreview.task.core.CallBackDAO;
import com.amazonreview.task.core.CassandraConfig;
import com.amazonreview.task.core.CassandraCqlDatastore;
import com.amazonreview.task.core.DSConfigDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

public class AmazonReviewUtility {
    private static final Logger logger = LoggerFactory.getLogger(AmazonReviewTaskDataGenerator.class);
    private DSConfigDAO dsConfigurationDAO = null;
    private CallBackDAO cbDAO =null;

    public AmazonReviewUtility(String cassandraHost, int cassandraPort, String keySpace){
        CassandraCqlDatastore cqlds = new CassandraCqlDatastore(new CassandraConfig(cassandraHost, "", keySpace, cassandraPort));
        dsConfigurationDAO = new DSConfigDAO(cqlds);
        cbDAO = new CallBackDAO(cqlds);
        try {
            cbDAO.register();
            dsConfigurationDAO.register();
        } catch (ConnectionException e) {
            logger.error(AmazonReviewError.TakeAwayError0.getErrorMsg(),e);
        }
    }
    /**
     * populate cassandra db for all data.
     */
    public void populateCSDB(Map<String,Object> data) {
        logger.info("Start of populateCSDB on timestamp: {}",new Timestamp(System.currentTimeMillis()));
         try {
            cbDAO.populateAllRecords(data);
        }catch(Exception ex) {
            logger.error("Failed to bulk populate Cassandra db",ex);
        }
        logger.info("End of populateCSDB on timestamp: {}",new Timestamp(System.currentTimeMillis()));
    }

    /**
     * fetch cassandra data with select query.
      */
    public List<Map<String, Object>> selectDataUtil(String table,int ratmonth) {
        List<Map<String, Object>> data = null ;
        logger.info("Start of selectDataUtil on timestamp: {}",new Timestamp(System.currentTimeMillis()));
        try {
            data = cbDAO.selectData(table,ratmonth,true);
        }catch(Exception ex) {
            logger.error("Failed to select data from Cassandra db",ex);
        }
        logger.info("End of selectDataUtil on timestamp: {}",new Timestamp(System.currentTimeMillis()));
        return data;
    }

}
