package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Insert.java   0.2.0   12/20/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.2.0
 * @since     0.2.0
 */

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.Properties;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Insert {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Insert(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.insert.db", "training");
        this.collectionName = properties.getProperty("mongodb.insert.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning insert operations...");

        final var collections = new Collections(this.mongoClient);

        collections.ensureCollection(this.dbName, this.collectionName);
        collections.dropCollection(this.dbName, this.collectionName);

        this.logger.info("Ending insert operations.");
        this.logger.exit();
    }
}
