package com.amazonreview.task.core;


public class CassandraConfig {

    /**
     * Instantiates a new cassandra config.
     *
     * @param nodeList the node list
     */
    public CassandraConfig(String nodeList) {
        this.nodeList = nodeList;
    }

    /**
     * Instantiates a new cassandra config.
     *
     * @param nodeList     the node list
     * @param columnFamily the column family
     * @param keySpace     the key space
     * @param port         the port
     */
    public CassandraConfig(String nodeList, String columnFamily, String keySpace, int port) {
        this.nodeList = nodeList;
        this.columnFamily = columnFamily;
        this.keySpace = keySpace;
        this.port = port;
    }

    /**
     * Instantiates a new cassandra config.
     *
     * @param nodeList     the node list
     * @param columnFamily the column family
     * @param keySpace     the key space
     */
    public CassandraConfig(String nodeList, String columnFamily, String keySpace) {
        this.nodeList = nodeList;
        this.columnFamily = columnFamily;
        this.keySpace = keySpace;
    }

    /**
     * Sets the node list.
     *
     * @param nodeList the new node list
     */
    public void setNodeList(String nodeList) {
        this.nodeList = nodeList;
    }

    /**
     * Gets the column family.
     *
     * @return the column family
     */
    public String getColumnFamily() {
        return columnFamily;
    }

    /**
     * Sets the column family.
     *
     * @param columnFamily the new column family
     */
    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    /**
     * Gets the key space.
     *
     * @return the key space
     */
    public String getKeySpace() {
        return keySpace;
    }

    /**
     * Sets the key space.
     *
     * @param keySpace the new key space
     */
    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port.
     *
     * @param port the new port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets the batch size.
     *
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size.
     *
     * @param batchSize the new batch size
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Gets the node list.
     *
     * @return the node list
     */
    public String getNodeList() {
        return nodeList;
    }

    /**
     * The node list.
     */
    private String nodeList = "localhost";

    /**
     * The key space.
     */
    private String columnFamily, keySpace;

    /**
     * The port.
     */
    private int port;

    /**
     * The batch size.
     */
    private int batchSize;
}
