package com.amazonreview.task;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraSampleConnectionTest {




    public static void main(String[] args) {
        CassandraSampleConnectionTest cc = new CassandraSampleConnectionTest();
        cc.execute();
    }

    public void execute() {
        Cluster cluster = Cluster.builder()
                .addContactPoints("localhost")
                .withPort(9042)
                .build();
        Session session = cluster.connect();
        String command = "drop keyspace if exists takeaway1";
        session.execute(command);
        cluster.close();
    }
}