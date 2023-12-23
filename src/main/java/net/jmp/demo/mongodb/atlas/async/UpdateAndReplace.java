package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)UpdateAndReplace.java 0.3.0   12/21/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.3.0
 * @since     0.3.0
 */

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import com.mongodb.client.result.UpdateResult;

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.Date;
import java.util.Properties;

import org.bson.Document;

import org.bson.conversions.Bson;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class UpdateAndReplace {
    enum UpdateType {
        ONE,
        MANY,
        REPLACE
    }

    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    UpdateAndReplace(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.update.db", "training");
        this.collectionName = properties.getProperty("mongodb.update.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning update and replace operations...");

        this.updateOneDocument();
        this.updateMultipleDocuments();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.updateOneDocumentAddField();
        this.updateMultipleDocumentsAddField();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.replaceOneDocument();

        Helpers.printOneDocument(this.mongoClient,
                this.dbName,
                this.collectionName,
                Filters.eq("color", "pink"),
                this.logger);

        this.logger.info("Ending update and replace operations.");
        this.logger.exit();
    }

    private void updateOneDocument() {
        this.logger.entry();

        final var filter = Filters.eq("color", "orange");
        final var update = Updates.mul("qty", 10);

        this.update(UpdateType.ONE, filter, update);

        this.logger.exit();
    }

    private void updateMultipleDocuments() {
        this.logger.entry();

        final var filter = Filters.empty(); // Another way to handle all documents
        final var update = Updates.inc("qty", 10);

        this.update(UpdateType.MANY, filter, update);

        this.logger.exit();
    }

    private void updateOneDocumentAddField() {
        this.logger.entry();

        final var filter = Filters.eq("color", "red");
        final var updateDocument = new Document("$set", new Document("comment", "This field was added on update"));

        this.update(UpdateType.ONE, filter, updateDocument);

        this.logger.exit();
    }

    private void updateMultipleDocumentsAddField() {
        this.logger.entry();

        final var filter = Filters.empty();
        final var updateDocument = new Document("$set", new Document("datetime", new Date()));

        this.update(UpdateType.MANY, filter, updateDocument);

        this.logger.exit();
    }

    private void replaceOneDocument() {
        this.logger.entry();

        final var filter = Filters.eq("color", "yellow");
        final var newDocument = new Document()
                .append("color", "pink")
                .append("quantity", 45)
                .append("comment", "This document replaced the yellow one")
                .append("datetime", new Date());

        this.update(UpdateType.REPLACE, filter, newDocument);

        this.logger.exit();
    }

    private void update(final UpdateType updateType, final Bson filter, final Document document) {
        this.logger.entry(updateType, filter, document);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();

        if (updateType == UpdateType.REPLACE)
            collection.replaceOne(filter, document).subscribe(updateSubscriber);
        else if (updateType == UpdateType.MANY)
            collection.updateMany(filter, document).subscribe(updateSubscriber);
        else if (updateType == UpdateType.ONE)
            collection.updateOne(filter, document).subscribe(updateSubscriber);
        else
            throw new IllegalArgumentException("Unrecognized update type: " + updateType.name());

        updateSubscriber.await();

        this.logSubscriberResults(updateSubscriber);

        this.logger.exit();
    }

    private void update(final UpdateType updateType, final Bson filter, final Bson update) {
        this.logger.entry(updateType, filter, update);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();

        if (updateType == UpdateType.MANY)
            collection.updateMany(filter, update).subscribe(updateSubscriber);
        else if (updateType == UpdateType.ONE)
            collection.updateOne(filter, update).subscribe(updateSubscriber);
        else
            throw new IllegalArgumentException("Unrecognized update type: " + updateType.name());

        updateSubscriber.await();

        this.logSubscriberResults(updateSubscriber);

        this.logger.exit();
    }

    private void logSubscriberResults(final ObservableSubscriber<UpdateResult> subscriber) {
        this.logger.entry(subscriber);

        if (subscriber.getError() == null) {
            final var result = subscriber.get().getFirst();

            if (this.logger.isInfoEnabled()) {
                this.logger.info("{} document(s) were matched", result.getMatchedCount());
                this.logger.info("{} document(s) were updated", result.getModifiedCount());
            }
        } else {
            this.logger.error(subscriber.getError().getMessage());
        }

        this.logger.exit();
    }
}
