package com.amazonreview.task;

import com.amazonreview.task.constants.AmazonReviewConstants;
import com.amazonreview.task.error.AmazonReviewError;
import com.amazonreview.task.util.AmazonReviewUtility;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class AmazonReviewTaskDataConsumer {
    private static AmazonReviewUtility takeawayUtil;
    private static final Logger logger = LoggerFactory.getLogger(AmazonReviewTaskDataGenerator.class);
    public static void main(String[] args){
        BasicConfigurator.configure();
        logger.info("Starting of TakeAway DC App...");
        String cassanadraHost = System.getenv(AmazonReviewConstants.CASSANDRA_HOST);
        String cassanadraPort = System.getenv(AmazonReviewConstants.CASSANDRA_PORT);
        String cassanadraKeySpace = System.getenv(AmazonReviewConstants.CASSANDRA_KEYSPACE);
        logger.info("Cassandra Host {} , Cassandra Port {}, Cassandra KeySpace {}",cassanadraHost,cassanadraPort,cassanadraKeySpace);
        if( cassanadraHost==null || cassanadraPort==null || cassanadraKeySpace==null) {
            logger.error(AmazonReviewError.TakeAwayError0.getErrorMsg());
            logger.error("'cassandraHost','cassandraPort','cassandraKeySpace' are to be set");
            stopServer();
        }
        initCassandraUtility(cassanadraHost, cassanadraPort, cassanadraKeySpace);
        dataFetch(Integer.valueOf(args[0]),Integer.valueOf(args[1]));

        logger.info("End of TakeAway DC App...");
        stopServer();
    }

    /**
     * Stopping Server
     */
    public static void stopServer() {
        logger.info("Shutting down TakeAway DC App...");
        System.exit(-1);
    }
    /**
     * Initialize Cassandra Utility
     * @param cassanadraHost -- Cassandra Host
     * @param cassanadraPort -- Cassandra Port
     * @param cassanadraKeySpace -- Cassandra Key Space
     */
    private static synchronized void initCassandraUtility(String cassanadraHost,String cassanadraPort,String cassanadraKeySpace) {
        takeawayUtil = new AmazonReviewUtility(cassanadraHost,Integer.parseInt(cassanadraPort),cassanadraKeySpace);
    }
    /**
     * To fetch data from cassandra
     */
    private static void dataFetch(int ratmonth, int limit){
        List<Map<String, Object>> data = null ;
        List<Map<String, Object>> data_prevMonth = null ;
        List<Map<String, Object>> data_combined = null;
        logger.info("Entry of dataFetch");
        try{
            data = takeawayUtil.selectDataUtil(AmazonReviewConstants.CASSANDRA_TAB_NAME_MOVIE_RATING,ratmonth);
        }catch (Exception e){
            logger.error("Failed to fetch data {}",e.getMessage());
        }
        logger.info("Result size {}",data.size());
        logger.debug("Full Data {}",data.toString());

        ascendingMovieOrder(data.stream().distinct().collect(Collectors.toList()), limit);
        descendingMovieOrder(data.stream().distinct().collect(Collectors.toList()), limit);

        try{
            data_prevMonth = takeawayUtil.selectDataUtil(AmazonReviewConstants.CASSANDRA_TAB_NAME_MOVIE_RATING,ratmonth-1);
        }catch (Exception e){
            logger.error("Failed to fetch data {}",e.getMessage());
        }
        logger.info("Prev Month Result size {}",data_prevMonth.size());
        data_combined = createCombinedData(data.stream().distinct().collect(Collectors.toList()), data_prevMonth.stream().distinct().collect(Collectors.toList()));
        logger.info("Total matched {}",data_combined.size());
        descendingMovieOrder(data_combined,limit);
        logger.info("End of dataFetch");
    }

    public static List<Map<String,Object>> createCombinedData(List<Map<String, Object>> data, List<Map<String,Object>> data_prevMonth)
    {
        logger.info("Start of createCombinedData on timestamp: {}",new Timestamp(System.currentTimeMillis()));
        List<Map<String, Object>> data_combined = null;
        try{
            data_combined = data.stream()
                    .filter(two -> data_prevMonth.stream()
                            .anyMatch(
                                    one -> one.get("item").equals(two.get("item"))
                                            && Double.compare(Double.parseDouble(one.get("avgrat").toString()),Double.parseDouble(two.get("avgrat").toString())) > 0
                            )
                    )
                    .collect(Collectors.toList());
        }catch(Exception e){
            logger.error("Failed to combined two collection{}",e.getMessage());
        }
        logger.info("End of createCombinedData on timestamp: {}",new Timestamp(System.currentTimeMillis()));
        return data_combined;
    }

    /**
     * Ascending Order of Movies
     */
    private static void ascendingMovieOrder(List<Map<String, Object>> rawData, int limit){
        logger.info("Start of ascendingMovieOrder");
        try{
            Collections.sort(rawData, mapComparator);
        }catch (Exception e){
            logger.error("Error in data sorting {}",e.getMessage());
        }
        logger.debug("Default sort {}",rawData.toString());
        logger.info("Movie Items(with lower rating) - {} having limit {}",rawData.stream().limit(limit).collect(Collectors.toList()),limit);
        logger.info("End of ascendingMovieOrder");
    }
    /**
     * Descending Order of Movies
     */
    private static void descendingMovieOrder(List<Map<String, Object>> rawData, int limit){
        logger.info("Start of descendingMovieOrder");
        try{
            Collections.sort(rawData,mapComparator.reversed());
        }catch (Exception e){
            logger.error("Error in data sorting {}",e.getMessage());
        }
        logger.debug("Reverse sort {}",rawData.toString());
        logger.info("Movie Items(with higher rating) - {} having limit {}",rawData.stream().limit(limit).collect(Collectors.toList()),limit);
        logger.info("End of descendingMovieOrder");
    }
    /**
     * Comparator for sorting data collection
     */
    public static Comparator<Map<String, Object>> mapComparator = new Comparator<Map<String, Object>>() {
        public int compare(Map<String, Object> m1, Map<String, Object> m2) {
            return Double.valueOf(m1.get(AmazonReviewConstants.ADDITIONAL_FIELD_RATING_VAL_AVG).toString())
                    .compareTo(Double.valueOf(m2.get(AmazonReviewConstants.ADDITIONAL_FIELD_RATING_VAL_AVG).toString()));
        }
    };
}
