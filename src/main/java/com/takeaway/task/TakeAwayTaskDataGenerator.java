package com.takeaway.task;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.takeaway.task.constants.TakeAwayConstants;
import com.takeaway.task.error.TakeAwayError;
import com.takeaway.task.util.TakeAwayUtility;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static java.time.ZonedDateTime.now;


public class TakeAwayTaskDataGenerator {
    private static TakeAwayUtility takeawayUtil;
    private static final Logger logger = LoggerFactory.getLogger(TakeAwayTaskDataGenerator.class);
    public static void main(String[] args){
        BasicConfigurator.configure();
        logger.info("Starting of TakeAway DG App...");
        String cassanadraHost = System.getenv(TakeAwayConstants.CASSANDRA_HOST);
        String cassanadraPort = System.getenv(TakeAwayConstants.CASSANDRA_PORT);
        String cassanadraKeySpace = System.getenv(TakeAwayConstants.CASSANDRA_KEYSPACE);
        logger.info("Cassandra Host {} , Cassandra Port {}, Cassandra KeySpace {}",cassanadraHost,cassanadraPort,cassanadraKeySpace);
        if( cassanadraHost==null || cassanadraPort==null || cassanadraKeySpace==null) {
            logger.error(TakeAwayError.TakeAwayError0.getErrorMsg());
            logger.error("'cassandraHost','cassandraPort','cassandraKeySpace' are to be set");
            stopServer();
        }
        initCassandraUtility(cassanadraHost, cassanadraPort, cassanadraKeySpace);
        dataProcess(args[0],args[1]);
        logger.info("End of TakeAway DG App...");
        stopServer();
    }

    /**
     * Download Amazon review data from internet
     */
    public static void downloadFile(String urlLocation, String csvLocation){
        try {
            logger.info("Starting download {}", now());
            URL website = new URL(urlLocation);
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream(csvLocation);
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            logger.info("Download Finished {}",now());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
     * Addind header info in csv file
     */
    public static void addHeader(String csvLocation){
        try {
            logger.info("Adding header {}",now());
            Path filePath = Paths.get(csvLocation);
            List<String> lines = Files.readAllLines(filePath);
            lines.add(0, TakeAwayConstants.CSV_HEADER);
            Files.write(filePath, lines);
            logger.info("Finished header {}",now());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
     * Parsing csv
     */
    public static List<Map<String, Object>> readObjectsFromCsv(File file) throws IOException {
        CsvSchema bootstrap = CsvSchema.emptySchema().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map<String, Object>> mappingIterator = csvMapper.reader(Map.class).with(bootstrap).readValues(file);
        return mappingIterator.readAll();
    }
    /**
     * Fetching Date in yyyy-MM-dd format
     */
    public static String getDateFromEpoch(Long epoch){
        ZonedDateTime dateTime = Instant.ofEpochSecond(epoch).atZone(ZoneId.of(TakeAwayConstants.TAKEAWAY_ZONE));
        return dateTime.format(DateTimeFormatter.ofPattern(TakeAwayConstants.TAKEAWAY_DATE_FORMAT));
    }
    /**
     * Fetching Month
     */
    public static int getMonthFromEpoch(Long epoch){
        ZonedDateTime dateTime = Instant.ofEpochSecond(epoch).atZone(ZoneId.of(TakeAwayConstants.TAKEAWAY_ZONE));
        LocalDate localDate = LocalDate.parse( dateTime.format(DateTimeFormatter.ofPattern(TakeAwayConstants.TAKEAWAY_DATE_FORMAT)),
                DateTimeFormatter.ofPattern(TakeAwayConstants.TAKEAWAY_DATE_FORMAT) );
        return localDate.getMonthValue();
    }
    /**
     * Fetching Year
     */
    public static int getYearFromEpoch(Long epoch){
        ZonedDateTime dateTime = Instant.ofEpochSecond(epoch).atZone(ZoneId.of(TakeAwayConstants.TAKEAWAY_ZONE));
        LocalDate localDate = LocalDate.parse( dateTime.format(DateTimeFormatter.ofPattern(TakeAwayConstants.TAKEAWAY_DATE_FORMAT)),
                DateTimeFormatter.ofPattern(TakeAwayConstants.TAKEAWAY_DATE_FORMAT) );
        return localDate.getYear();
    }
    /**
     * Stopping Server
     */
    public static void stopServer() {
        logger.info("Shutting down TakeAway DG App...");
        System.exit(-1);
    }
    /**
     * Initialize Cassandra Utility
     * @param cassanadraHost -- Cassandra Host
     * @param cassanadraPort -- Cassandra Port
     * @param cassanadraKeySpace -- Cassandra Key Space
     */
    private static synchronized void initCassandraUtility(String cassanadraHost,String cassanadraPort,String cassanadraKeySpace) {
        takeawayUtil = new TakeAwayUtility(cassanadraHost,Integer.parseInt(cassanadraPort),cassanadraKeySpace);
    }
    /**
     * To download data from url
     * then add the csv head
     * then parse the csv
     * then insert data to cassandra
     */
    private static void dataProcess(String urlLocation, String csvLocation){
        logger.info("Entry of dataProcess");
        downloadFile(urlLocation,csvLocation);
        addHeader(csvLocation);
        File csvFile = new File(csvLocation);
        try {
            List<Map<String, Object>> datas = readObjectsFromCsv(csvFile);
            logger.info("Total rows {}", datas.size());
            datas.stream().forEach((data) -> {
                try {
                    data.put(TakeAwayConstants.ADDITIONAL_FIELD_DATE, getDateFromEpoch(Long.valueOf(data.get(TakeAwayConstants.FIELD_TIMESTAMP_NAME).toString())));
                    data.put(TakeAwayConstants.ADDITIONAL_FIELD_MONTH, getMonthFromEpoch(Long.valueOf(data.get(TakeAwayConstants.FIELD_TIMESTAMP_NAME).toString())));
                    data.put(TakeAwayConstants.ADDITIONAL_FIELD_YEAR, getYearFromEpoch(Long.valueOf(data.get(TakeAwayConstants.FIELD_TIMESTAMP_NAME).toString())));
                    data.put(TakeAwayConstants.ADDITIONAL_FIELD_RATING_VAL, Double.valueOf(data.get(TakeAwayConstants.FIELD_RATING_NAME).toString()));
                    takeawayUtil.populateCSDB(data);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }catch (IOException e){
            logger.error("Error in Data Process {}",e.getMessage());
        }
        logger.info("End of dataProcess");
    }
}
