package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Helpers.java  0.3.0   12/22/2023
 * (#)Helpers.java  0.2.0   12/21/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.3.0
 * @since     0.2.0
 */

import com.mongodb.reactivestreams.client.MongoClient;

import org.bson.conversions.Bson;

import org.slf4j.ext.XLogger;

final class Helpers {
    private Helpers() {
        super();
    }

    static void printAllDocuments(final MongoClient mongoClient,
                                  final String databaseName,
                                  final String collectionName,
                                  final XLogger logger) {
        logger.entry(mongoClient, databaseName, collectionName);

        final var database = mongoClient.getDatabase(databaseName);
        final var collection = database.getCollection(collectionName);

        final var documentSubscriber = new PrintDocumentSubscriber(logger);

        collection.find().subscribe(documentSubscriber);
        documentSubscriber.await();

        logger.exit();
    }

    static void printOneDocument(final MongoClient mongoClient,
                                 final String databaseName,
                                 final String collectionName,
                                 final Bson filter,
                                 final XLogger logger) {
        logger.entry(mongoClient, databaseName, collectionName, filter);

        final var documentSubscriber = new PrintDocumentSubscriber(logger);

        final var database = mongoClient.getDatabase(databaseName);
        final var collection = database.getCollection(collectionName);

        collection.find(filter)
                .first()
                .subscribe(documentSubscriber);

        documentSubscriber.await();

        logger.exit();
    }
}
