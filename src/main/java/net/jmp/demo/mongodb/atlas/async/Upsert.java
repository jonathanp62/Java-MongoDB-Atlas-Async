package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Upsert.java   0.6.0   01/09/2024
 *
 * @author    Jonathan Parker
 * @version   0.6.0
 * @since     0.6.0
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

import com.mongodb.client.model.*;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.*;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Upsert {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Upsert(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.upsert.db", "training");
        this.collectionName = properties.getProperty("mongodb.upsert.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning upsert operations...");

        this.insertData();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.upsertThatInserts();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.upsertThatUpdates();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.deleteData();

        this.logger.info("Ending upsert operations...");
        this.logger.exit();
    }

    private void insertData() {
        this.logger.entry();

        final var color = "color";
        final var quantity = "quantity";
        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var documents = List.of(
                new Document(color, "red").append(quantity, 5),
                new Document(color, "purple").append(quantity, 8),
                new Document(color, "blue").append(quantity, 0),
                new Document(color, "white").append(quantity, 0),
                new Document(color, "yellow").append(quantity, 6),
                new Document(color, "pink").append(quantity, 0),
                new Document(color, "green").append(quantity, 0),
                new Document(color, "black").append(quantity, 8)
        );

        final ObservableSubscriber<InsertManyResult> subscriber = new OperationSubscriber<>();

        collection.insertMany(documents).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            subscriber.first().getInsertedIds().values()
                    .forEach(id -> this.logger.info("Inserted document: {}", id.asObjectId().getValue()));
        } else {
            final var error = subscriber.getError();

            if (error instanceof MongoBulkWriteException mbwe) {
                mbwe.getWriteResult().getInserts()
                        .forEach(doc -> this.logger.info("Inserted document: {}", doc.getId().asObjectId().getValue()));
            }

            this.logger.catching(error);
        }

        this.logger.exit();
    }

    private void upsertThatInserts() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "orange");
        final var update = Updates.inc("quantity", 10);
        final var options = new UpdateOptions().upsert(true);

        final ObservableSubscriber<UpdateResult> subscriber = new OperationSubscriber<>();

        collection.updateOne(filter, update, options).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            this.logger.info("{}", subscriber.first());
        } else {
            this.logger.throwing(subscriber.getError());
        }

        this.logger.exit();
    }

    private void upsertThatUpdates() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "orange");
        final var update = Updates.inc("quantity", 15);
        final var options = new UpdateOptions().upsert(true);

        final ObservableSubscriber<UpdateResult> subscriber = new OperationSubscriber<>();

        collection.updateOne(filter, update, options).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            this.logger.info("{}", subscriber.first());
        } else {
            this.logger.catching(subscriber.getError());
        }

        this.logger.exit();
    }

    private void deleteData() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.empty();

        final ObservableSubscriber<DeleteResult> subscriber = new OperationSubscriber<>();

        collection.deleteMany(filter).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            this.logger.info("{} document(s) were deleted", subscriber.first().getDeletedCount());
        } else {
            this.logger.catching(subscriber.getError());
        }

        this.logger.exit();
    }
}
