package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Bulk.java 0.7.0   01/09/2024
 *
 * @author    Jonathan Parker
 * @version   0.7.0
 * @since     0.7.0
 *
 * MIT License
 *
 * Copyright (c) 2024 Jonathan M. Parker
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import com.mongodb.MongoBulkWriteException;

import com.mongodb.bulk.BulkWriteResult;

import com.mongodb.client.model.*;

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Bulk {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Bulk(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.bulk.db", "training");
        this.collectionName = properties.getProperty("mongodb.bulk.collection", "people");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning bulk operations...");

        this.insert();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.replace();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.update();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.delete();

        this.logger.info("Ending bulk operations...");
        this.logger.exit();
    }

    private void insert() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var karenDoc = new InsertOneModel<>(new Document("name", "Karen Sandoval").append("age", 31));
        final var williamDoc = new InsertOneModel<>(new Document("name", "William Chin").append("age", 54));
        final var shaylaDoc = new InsertOneModel<>(new Document("name", "Shayla Ray").append("age", 20));
        final var juneDoc = new InsertOneModel<>(new Document("name", "June Carrie").append("age", 17));
        final var kevinDoc = new InsertOneModel<>(new Document("name", "Kevin Moss").append("age", 22));

        final List<WriteModel<Document>> bulkDocuments = new ArrayList<>();

        bulkDocuments.add(karenDoc);
        bulkDocuments.add(williamDoc);
        bulkDocuments.add(shaylaDoc);
        bulkDocuments.add(juneDoc);
        bulkDocuments.add(kevinDoc);

        final var options = new BulkWriteOptions().ordered(true);

        final ObservableSubscriber<BulkWriteResult> subscriber = new OperationSubscriber<>();

        collection.bulkWrite(bulkDocuments, options).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            this.logger.info("Documents inserted: {}", subscriber.first().getInsertedCount());
        } else {
            final var error = subscriber.getError();

            if (error instanceof MongoBulkWriteException mbwe) {
                mbwe.getWriteResult().getInserts()
                        .forEach(doc -> this.logger.info("Inserted document: {}", doc.getId().asObjectId().getValue()));
            }

            this.logger.error(error.getMessage());
        }

        this.logger.exit();
    }

    private void replace() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("name", "William Chin");

        final var marinaDoc = new ReplaceOneModel<>(filter,
                new Document("name", "Marina Squerciati")
                        .append("age", 39)
                        .append("location", "New York, NY"));

        final List<WriteModel<Document>> bulkDocuments = List.of(marinaDoc);

        final ObservableSubscriber<BulkWriteResult> subscriber = new OperationSubscriber<>();

        collection.bulkWrite(bulkDocuments).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            final var result = subscriber.first();

            if (this.logger.isInfoEnabled()) {
                this.logger.info("Documents matched: {}", result.getMatchedCount());
                this.logger.info("Documents modified: {}", result.getModifiedCount());
            }
        } else {
            final var error = subscriber.getError();

            if (error instanceof MongoBulkWriteException mbwe) {
                mbwe.getWriteResult().getUpserts()
                        .forEach(doc -> this.logger.info("Upserted document: {}", doc.getId().asObjectId().getValue()));
            }

            this.logger.error(error.getMessage());
        }

        this.logger.exit();
    }

    private void update() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("name", "June Carrie");
        final var update = Updates.set("age", 19);

        final UpdateOneModel<Document> juneDoc = new UpdateOneModel<>(filter, update);

        final List<UpdateOneModel<Document>> bulkDocuments = List.of(juneDoc);

        final ObservableSubscriber<BulkWriteResult> subscriber = new OperationSubscriber<>();

        collection.bulkWrite(bulkDocuments).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            final var result = subscriber.first();

            if (this.logger.isInfoEnabled()) {
                this.logger.info("Documents matched: {}", result.getMatchedCount());
                this.logger.info("Documents modified: {}", result.getModifiedCount());
            }
        } else {
            final var error = subscriber.getError();

            if (error instanceof MongoBulkWriteException mbwe) {
                mbwe.getWriteResult().getUpserts()
                        .forEach(doc -> this.logger.info("Upserted document: {}", doc.getId().asObjectId().getValue()));
            }

            this.logger.error(error.getMessage());
        }

        this.logger.exit();
    }

    private void delete() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.empty();

        final DeleteManyModel<Document> deleteDocs = new DeleteManyModel<>(filter);

        final List<DeleteManyModel<Document>> bulkDocuments = List.of(deleteDocs);

        final var options = new BulkWriteOptions().ordered(false);

        final ObservableSubscriber<BulkWriteResult> subscriber = new OperationSubscriber<>();

        try {
            collection.bulkWrite(bulkDocuments, options).subscribe(subscriber);

            subscriber.await();

            if (subscriber.getError() == null) {
                this.logger.info("Documents deleted: {}", subscriber.first().getDeletedCount());
            } else {
                final var error = subscriber.getError();

                if (error instanceof MongoBulkWriteException mbwe)
                    this.logger.warn("Documents deleted: {}", mbwe.getWriteResult().getDeletedCount());

                this.logger.error(error.getMessage());
            }
        } catch (final MongoBulkWriteException mbwe) {
            this.logger.catching(mbwe);
        }

        this.logger.exit();
    }
}
