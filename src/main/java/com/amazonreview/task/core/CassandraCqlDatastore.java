package com.amazonreview.task.core;

import com.amazonreview.task.constants.AmazonReviewConstants;
import com.amazonreview.task.exception.ConnectionException;
import com.amazonreview.task.exception.DataAccessException;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import com.datastax.driver.core.ColumnDefinitions.Definition;

public class CassandraCqlDatastore implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -7189958036429001543L;

    /** The cluster. */
    private transient Cluster cluster;

    /** The session. */
    private static Session session;

    /** The config. */
    private transient CassandraConfig config;

    /** The write consistency. */
    private ConsistencyLevel writeConsistency;

    /** The read consistency. */
    private ConsistencyLevel readConsistency;

    /** The Constant log. */
    private static final Logger logger = LoggerFactory.getLogger(CassandraCqlDatastore.class);

    /** The is cluster created. */
    private static boolean isClusterCreated = false;

    /** The Constant FAILED_MESSAGE. */
    private static final String FAILED_MESSAGE = "Failed to write in data store";

    /** The Constant WHERE. */
    private static final String WHERE = " WHERE ";

    /**
     * Instantiates a new cassandra CQL ds.
     *
     * @param config the config
     */
    public CassandraCqlDatastore(CassandraConfig config) {
        this.config = config;
        setAdditionalConfiguration();
    }

    /**
     * Additional information for connection pooling.
     */
    private void setAdditionalConfiguration() {
        String write = System.getenv("cassandraWriteConsistency");
        String read = System.getenv("cassandraReadConsistency");
        logger.info("cassandraWriteConsistency: {} && cassandraReadConsistency: {}", System.getenv("cassandraWriteConsistency")
                ,System.getenv("cassandraReadConsistency"));
        if (("ONE").equalsIgnoreCase(write)) {
            this.writeConsistency = ConsistencyLevel.ONE;
        } else if (("ALL").equalsIgnoreCase(write)) {
            this.writeConsistency = ConsistencyLevel.ALL;
        } else {
            this.writeConsistency = ConsistencyLevel.QUORUM;
        }
        if (("ONE").equals(read)) {
            this.readConsistency = ConsistencyLevel.ONE;
        } else if (("ALL").equalsIgnoreCase(read)) {
            this.readConsistency = ConsistencyLevel.ALL;
        } else {
            this.readConsistency = ConsistencyLevel.QUORUM;
        }
    }

    /**
     * Cassandra Method to connect.
     *
     * @throws ConnectionException the connection exception
     */
    public void connect() throws ConnectionException {
        try {
            buildConnectionPool();
        } catch (Exception e) {
            throw new ConnectionException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Close.
     */
    public void close() {
        // cluster not to be closed
        logger.info(" Doing nothing on close in CassandraCQLDs ");
    }

    /**
     * Method for Building Connection Pool.
     *
     * @throws ConnectionException the connection exception
     */
    private synchronized void buildConnectionPool() throws ConnectionException {
        logger.info(" Begining of method buildConnectionPool ");
        if (!isClusterCreated) {
            try {
                logger.info(" Building singleton session and connection pool ");
                // Take default value for Queue Size in case env. contains bad
                // value
                int sessionPoolQueueSize = PoolingOptions.DEFAULT_MAX_QUEUE_SIZE;
                // Fetch Queue Size of Connection Pool for Session from Env.
                String queueSizeEnv = System.getenv("sessionPoolQueueSize");
                try {
                    sessionPoolQueueSize = Integer.parseInt(queueSizeEnv);
                } catch (NumberFormatException nfExc) {
                    logger.info("Bad Value for Session Pool Queue Size: {}", queueSizeEnv);
                }
                logger.info("Pool Queue Size of : {} to be used", sessionPoolQueueSize);
                PoolingOptions poolingOptions = new PoolingOptions().setCoreConnectionsPerHost(HostDistance.LOCAL, 20)
                        .setCoreConnectionsPerHost(HostDistance.REMOTE, 20)
                        .setMaxConnectionsPerHost(HostDistance.LOCAL, 10000)
                        .setMaxConnectionsPerHost(HostDistance.REMOTE, 10000).setMaxQueueSize(sessionPoolQueueSize);
                Cluster.Builder clusterBuilder = Cluster.builder().addContactPoints(getServers(config.getNodeList()))
                        .withPort(config.getPort());
                clusterBuilder.withProtocolVersion(ProtocolVersion.V3).withPoolingOptions(poolingOptions);
                cluster = clusterBuilder.build();
                Metadata metadata = cluster.getMetadata();
                logger.info("Connected to cluster: {}", metadata.getClusterName());
                for (Host host : metadata.getAllHosts()) {
                    logger.info("Datacenter: {} && Host: {} && Rack: {}", host.getDatacenter(), host.getAddress(), host.getRack());
                }
                session = cluster.connect(config.getKeySpace());
                isClusterCreated = true;
            } catch (Exception e) {
                ConnectionException rce = new ConnectionException(e.getMessage());
                rce.initCause(e);
                throw rce;
            }
        }
        logger.info(" Exiting from buildConnectionPool method  ");
    }

    /**
     * Gets the servers.
     *
     * @param delimitedServerNames the delimited server names
     * @return Method Returns with an array of delimited servers with Comma
     */
    public String[] getServers(String delimitedServerNames) {
        logger.info(" Begining of method getServers ");
        String[] serverNames = null;
        if (delimitedServerNames != null && delimitedServerNames.contains(AmazonReviewConstants.CS_DELIMITER_COMMA)) {
            serverNames = delimitedServerNames.split(AmazonReviewConstants.CS_DELIMITER_COMMA);
        } else {
            serverNames = new String[1];
            serverNames[0] = delimitedServerNames;
        }
        logger.info("DelimitedServerNames: {}", Arrays.asList(serverNames));
        logger.info("Exiting of method getServers ");
        return serverNames;
    }

    /**
     * Write records.
     * @param table     the table
     * @param valueArgs the value args
     * @throws DataAccessException Write Records to Batch
     */
    public void writeRecords(String table, Map<String, Object> valueArgs) throws DataAccessException {
        logger.info(" Begining of method writeRecords ");
        try {
            Insert insert = QueryBuilder.insertInto(config.getKeySpace(), table);
            for (Map.Entry<String, Object> entry : valueArgs.entrySet()) {
                insert.value(entry.getKey(), entry.getValue());
            }
            insert.setConsistencyLevel(writeConsistency);
            session.execute(insert);
        } catch (Exception ex) {
            logger.error("Error In method writeRecords", ex);
            throw new DataAccessException(FAILED_MESSAGE, ex);
        }
        logger.info("Exiting of method writeRecords");
    }

    /**
     * Select Data using query.
     * @param table          the table
     * @param allowFiltering the allow filtering
     * @return Overloaded Version for Select Clause
     */
    public List<Map<String, Object>> selectQuery(String table,int ratmonth,boolean allowFiltering) {
        logger.info("Begining of method selectQuery");
        Select select = QueryBuilder.select().avg(AmazonReviewConstants.ADDITIONAL_FIELD_RATING_VAL).as(AmazonReviewConstants.ADDITIONAL_FIELD_RATING_VAL_AVG)
                .column(AmazonReviewConstants.FIELD_ITEM_NAME).from(config.getKeySpace(), table).where(QueryBuilder.eq(AmazonReviewConstants.ADDITIONAL_FIELD_MONTH,ratmonth))
                .groupBy(AmazonReviewConstants.FIELD_ITEM_NAME, AmazonReviewConstants.FIELD_USER_NAME);
        if (allowFiltering) {
            select.allowFiltering();
        }
        select.setConsistencyLevel(readConsistency);
        logger.info("Query to be execute {}",select.toString());
        ResultSet rs = session.execute(select);
        logger.info("Exiting of method selectQuery");
        return resultsetToList(rs);
    }

    /**
     * Resultset to list.
     * @param rs the rs
     * @return Convert Result set to List
     */
    public List<Map<String, Object>> resultsetToList(ResultSet rs) {
        logger.info(" Begining of method resultsetToList ");
        List<Map<String, Object>> data = new ArrayList<>();
        for (Iterator<Row> iter = rs.iterator(); iter.hasNext();) {
            Row row = iter.next();
            data.add(rowToMap(row));
        }
        logger.info(" exiting of method resultsetToList ");
        return data;
    }

    /**
     * Row to map.
     * @param row the row
     * @return Convert Row to MAp
     */
    private Map<String, Object> rowToMap(Row row) {
        logger.debug(" Start of method rowToMap ");
        Map<String, Object> rowMap = new HashMap<>();
        for (Definition def : row.getColumnDefinitions().asList()) {
            String name = def.getName();
            DataType typ = def.getType();
            ByteBuffer bytes = row.getBytesUnsafe(name);
            CodecRegistry codecregistry = session.getCluster().getConfiguration().getCodecRegistry();
            if (bytes != null) {
                TypeCodec<Object> registry = codecregistry.codecFor(typ);
                rowMap.put(name, registry.deserialize(bytes, ProtocolVersion.V3));
            } else {
                rowMap.put(name, null);
            }
        }
        logger.debug(" End of method rowToMap ");
        return rowMap;
    }

}
