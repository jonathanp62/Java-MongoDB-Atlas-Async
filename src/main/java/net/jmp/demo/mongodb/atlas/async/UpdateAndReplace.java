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

import com.mongodb.reactivestreams.client.MongoClient;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import com.mongodb.client.result.UpdateResult;

import java.util.Date;
import java.util.Properties;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class UpdateAndReplace {
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

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "orange");
        final var update = Updates.mul("qty", 10);

        ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();

        collection.updateOne(filter, update).subscribe(updateSubscriber);
        updateSubscriber.await();

        if (updateSubscriber.getError() == null) {
            final var result = updateSubscriber.get().getFirst();

            if (this.logger.isInfoEnabled()) {
                this.logger.info("{} document(s) were matched", result.getMatchedCount());
                this.logger.info("{} document(s) were updated", result.getModifiedCount());
            }
        } else {
            this.logger.error(updateSubscriber.getError().getMessage());
        }

        this.logger.exit();
    }

    private void updateMultipleDocuments() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.empty(); // Another way to handle all documents
        final var update = Updates.inc("qty", 10);

        ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();

        collection.updateMany(filter, update).subscribe(updateSubscriber);
        updateSubscriber.await();

        if (updateSubscriber.getError() == null) {
            final var result = updateSubscriber.get().getFirst();

            if (this.logger.isInfoEnabled()) {
                this.logger.info("{} document(s) were matched", result.getMatchedCount());
                this.logger.info("{} document(s) were updated", result.getModifiedCount());
            }
        } else {
            this.logger.error(updateSubscriber.getError().getMessage());
        }

        this.logger.exit();
    }

    private void updateOneDocumentAddField() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "red");
        final var updateDocument = new Document("$set", new Document("comment", "This field was added on update"));

        ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();

        collection.updateOne(filter, updateDocument).subscribe(updateSubscriber);
        updateSubscriber.await();

        if (updateSubscriber.getError() == null) {
            final var result = updateSubscriber.get().getFirst();

            if (this.logger.isInfoEnabled()) {
                this.logger.info("{} document(s) were matched", result.getMatchedCount());
                this.logger.info("{} document(s) were updated", result.getModifiedCount());
            }
        } else {
            this.logger.error(updateSubscriber.getError().getMessage());
        }

        this.logger.exit();
    }

    private void updateMultipleDocumentsAddField() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.empty();
        final var updateDocument = new Document("$set", new Document("datetime", new Date()));

        ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();

        collection.updateMany(filter, updateDocument).subscribe(updateSubscriber);
        updateSubscriber.await();

        if (updateSubscriber.getError() == null) {
            final var result = updateSubscriber.get().getFirst();

            if (this.logger.isInfoEnabled()) {
                this.logger.info("{} document(s) were matched", result.getMatchedCount());
                this.logger.info("{} document(s) were updated", result.getModifiedCount());
            }
        } else {
            this.logger.error(updateSubscriber.getError().getMessage());
        }

        this.logger.exit();
    }

    private void replaceOneDocument() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "yellow");
        final var newDocument = new Document()
                .append("color", "pink")
                .append("quantity", 45)
                .append("comment", "This document replaced the yellow one")
                .append("datetime", new Date());

        ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();

        collection.replaceOne(filter, newDocument).subscribe(updateSubscriber);
        updateSubscriber.await();

        if (updateSubscriber.getError() == null) {
            final var result = updateSubscriber.get().getFirst();

            if (this.logger.isInfoEnabled()) {
                this.logger.info("{} document(s) were matched", result.getMatchedCount());
                this.logger.info("{} document(s) were updated", result.getModifiedCount());
            }
        } else {
            this.logger.error(updateSubscriber.getError().getMessage());
        }

        this.logger.exit();
    }
}
