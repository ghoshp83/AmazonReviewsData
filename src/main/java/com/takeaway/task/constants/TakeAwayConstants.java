package com.takeaway.task.constants;

public class TakeAwayConstants {
    public static final String CSV_HEADER="user,item,rating,rattimestamp";
    public static final String TAKEAWAY_ZONE="GMT";
    public static final String TAKEAWAY_DATE_FORMAT="yyyy-MM-dd";
    public static final String FIELD_TIMESTAMP_NAME="rattimestamp";
    public static final String FIELD_RATING_NAME="rating";
    public static final String FIELD_ITEM_NAME="item";
    public static final String FIELD_USER_NAME="user";
    public static final String ADDITIONAL_FIELD_RATING_VAL="rating_val";
    public static final String ADDITIONAL_FIELD_RATING_VAL_AVG="avgrat";
    public static final String ADDITIONAL_FIELD_DATE="ratdate";
    public static final String ADDITIONAL_FIELD_MONTH="ratmonth";
    public static final String ADDITIONAL_FIELD_YEAR="ratyear";
    public static final String CS_DELIMITER_COMMA =",";
    public static final String CASSANDRA_HOST = "cassandraHost";
    public static final String CASSANDRA_PORT = "cassandraPort";
    public static final String CASSANDRA_KEYSPACE = "cassandraKeySpace";
    //public static final String CASSANDRA_TAB_NAME_MOVIE_RATING = "testratmtv_dd";
    public static final String CASSANDRA_TAB_NAME_MOVIE_RATING = "testratmtv_small";
}
