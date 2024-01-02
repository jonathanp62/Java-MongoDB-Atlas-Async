package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Delete.java   0.4.0   01/02/2024
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.4.0
 * @since     0.4.0
 */

import com.mongodb.client.model.Filters;

import com.mongodb.client.result.DeleteResult;

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.Properties;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Delete {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Delete(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.delete.db", "training");
        this.collectionName = properties.getProperty("mongodb.delete.collection", "colors");
    }
    void run() {
        this.logger.entry();
        this.logger.info("Beginning delete operations...");

        this.deleteOneDocument();
        this.findAndDeleteOneDocument();
        this.deleteMultipleDocuments();
        this.deleteAllDocuments();

        this.logger.info("Ending delete operations.");
        this.logger.exit();
    }

    private void deleteOneDocument() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "red");

        final ObservableSubscriber<DeleteResult> deleteSubscriber = new OperationSubscriber<>();

        collection.deleteOne(filter).subscribe(deleteSubscriber);

        deleteSubscriber.await();

        this.logger.info("{} document(s) were deleted", deleteSubscriber.first().getDeletedCount());

        this.logger.exit();
    }

    private void findAndDeleteOneDocument() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "orange");

        final ObservableSubscriber<Document> deleteSubscriber = new OperationSubscriber<>();

        collection.findOneAndDelete(filter).subscribe(deleteSubscriber);

        deleteSubscriber.await();

        final var document = deleteSubscriber.first();

        if (document != null) {
            if (this.logger.isInfoEnabled())
                this.logger.info("Deleted document: {}", document.toJson());
        } else {
            this.logger.warn("No documents with a color of orange were found");
        }

        this.logger.exit();
    }

    private void deleteMultipleDocuments() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("qty", 15);

        final ObservableSubscriber<DeleteResult> deleteSubscriber = new OperationSubscriber<>();

        collection.deleteMany(filter).subscribe(deleteSubscriber);

        deleteSubscriber.await();

        this.logger.info("{} document(s) were deleted", deleteSubscriber.first().getDeletedCount());

        this.logger.exit();
    }

    private void deleteAllDocuments() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final ObservableSubscriber<DeleteResult> deleteSubscriber = new OperationSubscriber<>();

        // An empty document as a filter will delete all documents

        collection.deleteMany(new Document()).subscribe(deleteSubscriber);

        deleteSubscriber.await();

        this.logger.info("{} document(s) were deleted", deleteSubscriber.first().getDeletedCount());

        this.logger.exit();
    }
}
