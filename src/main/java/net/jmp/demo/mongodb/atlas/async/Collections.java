package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Collections.java  0.7.0   01/09/2024
 * (#)Collections.java  0.2.0   12/20/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.7.0
 * @since     0.2.0
 */

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Collections {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;

    Collections(final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;
    }

    void ensureCollection(final String dbName, final String collectionName) {
        this.logger.entry(dbName, collectionName);

        if (!this.existsCollection(dbName, collectionName))
            this.createCollection(dbName, collectionName);

        this.logger.exit();
    }

    boolean existsCollection(final String dbName, final String collectionName) {
        this.logger.entry(dbName, collectionName);

        boolean result;

        final MongoDatabase database = this.mongoClient.getDatabase(dbName);
        final ObservableSubscriber<String> listSubscriber = new OperationSubscriber<>();

        database.listCollectionNames()
                .subscribe(listSubscriber);

        listSubscriber.await();

        if (listSubscriber.getError() == null) {
            final var collectionNames = listSubscriber.getReceived();

            if (collectionNames.contains(collectionName)) {
                result = true;

                this.logger.info("Collection {} exists in database {}", collectionName, dbName);
            } else {
                result = false;

                this.logger.warn("Collection {} does not exist in database {}", collectionName, dbName);
            }
        } else {
            final var error = listSubscriber.getError();

            this.logger.error(error.getMessage());

            throw new RuntimeException("Exception checking for collection " + collectionName, error);
        }

        this.logger.exit(result);

        return result;
    }

    private void createCollection(final String dbName, final String collectionName) {
        this.logger.entry(dbName, collectionName);

        final MongoDatabase database = this.mongoClient.getDatabase(dbName);
        final ObservableSubscriber<Void> voidSubscriber = new OperationSubscriber<>();

        database.createCollection(collectionName)
                .subscribe(voidSubscriber);

        voidSubscriber.await();

        if (voidSubscriber.getError() != null) {
            this.logger.error(voidSubscriber.getError().getMessage());
        } else {
            this.logger.info("Collection {} created in database {}", collectionName, dbName);
        }

        this.logger.exit();
    }

    void dropCollection(final String dbName, final String collectionName) {
        this.logger.entry(dbName, collectionName);

        final var database = this.mongoClient.getDatabase(dbName);
        final var collection = database.getCollection(collectionName);

        final ObservableSubscriber<Void> voidSubscriber = new OperationSubscriber<>();

        collection.drop()
                .subscribe(voidSubscriber);

        voidSubscriber.await();

        if (voidSubscriber.getError() != null) {
            this.logger.error(voidSubscriber.getError().getMessage());
        } else {
            this.logger.info("Collection {} dropped from database {}", collectionName, dbName);
        }

        this.logger.exit();
    }
}
