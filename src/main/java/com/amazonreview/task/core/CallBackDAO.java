package com.amazonreview.task.core;


import com.amazonreview.task.constants.AmazonReviewConstants;
import com.amazonreview.task.exception.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CallBackDAO {
    private static final Logger logger = LoggerFactory.getLogger(CallBackDAO.class);
    private CassandraCqlDatastore dao;
    public CallBackDAO(CassandraCqlDatastore dao) {
        this.dao = dao;
    }
    /**
     * Register.
     *
     * @throws ConnectionException the connection exception
     */
    public void register() throws ConnectionException {
        dao.connect();
    }

    /**
     * Unregister.
     */
    public void unregister(){
        dao.close();
    }

    /**
     * Populate all records.
     * @param valueArgs the value args
     * @return true, if successful
     */
    public boolean populateAllRecords(Map<String,Object> valueArgs) {
        logger.info("Entry of populateAllRecords");
        boolean result = false;
        try{
            dao.writeRecords(AmazonReviewConstants.CASSANDRA_TAB_NAME_MOVIE_RATING, valueArgs);
            result=true;
        }catch(Exception ex){
            logger.error("Failed to populate Rating data..."+ex);
        }
        logger.info("Exit of populateAllRecords");
        return result;
    }

    /**
     * Select records.
     * @param allowFiltering the allow filtering
     * @return list of map, if successful
     */
    public List<Map<String, Object>> selectData (String table,int ratmonth, boolean allowFiltering){
        logger.info("Entry of selectData");
        List<Map<String, Object>> data = null ;
        try{
            data = dao.selectQuery(table,ratmonth,allowFiltering);
        }catch (Exception ex){
            logger.error("Failed to select Rating data..."+ex);
        }
        logger.info("Exit of selectData");
        return data;
    }
}
