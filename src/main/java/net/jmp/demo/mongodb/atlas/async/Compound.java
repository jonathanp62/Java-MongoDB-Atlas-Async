package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Compopund.java    0.8.0   02/03/2024
 *
 * @author    Jonathan Parker
 * @version   0.8.0
 * @since     0.8.0
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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Compound {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    class DocumentPrinter extends ConsumerSubscriber<Document> {
        DocumentPrinter(final String methodName) {
            super(document -> {
                if (logger.isInfoEnabled())
                    logger.info("{}: {}", methodName, document.toJson());
            });
        }
    }

    Compound(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.compound.db", "training");
        this.collectionName = properties.getProperty("mongodb.compound.collection", "food");
    }

    void run() {
        this.logger.entry();

        this.insertData();

        try {
            this.findAndUpdate();
            this.findAndReplace();
            this.findAndDelete();

            this.raceCondition();
            this.resetRaceCondition();
            this.noRaceCondition();
        } finally {
            this.deleteData();
        }

        this.logger.exit();
    }

    private void insertData() {
        this.logger.entry();

        final var jsonDocuments = List.of(
                "{ \"_id\": 1, \"food\": \"donut\", \"color\": \"green\" }",
                "{ \"_id\": 2, \"food\": \"pear\", \"color\": \"yellow\" }",
                "{ \"_id\": 3, \"guest\": null, \"room\": \" Blue Room\", \"reserved\": false }"
        );

        final List<Document> documents = new ArrayList<>();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        jsonDocuments.forEach(jsonDocument -> documents.add(Document.parse(jsonDocument)));

        final ObservableSubscriber<InsertManyResult> subscriber = new OperationSubscriber<>();

        collection.insertMany(documents).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            subscriber.first().getInsertedIds().values()
                    .forEach(id -> this.logger.info("Inserted document: {}", id.asInt32().getValue()));
        } else {
            final var error = subscriber.getError();

            if (error instanceof MongoBulkWriteException mbwe) {
                mbwe.getWriteResult().getInserts()
                        .forEach(doc -> this.logger.info("Inserted document: {}", doc.getId().asInt32().getValue()));
            }

            this.logger.error(error.getMessage());
        }

        this.logger.exit();
    }

    private void findAndUpdate() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "green");
        final var projection = Projections.excludeId();
        final var update = Updates.set("food", "pizza");

        final var options = new FindOneAndUpdateOptions().
                projection(projection).
                upsert(true).
                maxTime(5, TimeUnit.SECONDS).
                returnDocument(ReturnDocument.AFTER);

        // The found document is in the state AFTER the update

        final var subscriber = new DocumentPrinter("findAndUpdate");

        collection
                .findOneAndUpdate(filter, update, options)
                .subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void findAndReplace() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("color", "green");
        final var projection = Projections.excludeId();
        final var newDocument = new Document("music", "classical").append("color", "green");

        final var options = new FindOneAndReplaceOptions().
                projection(projection).
                upsert(true).
                maxTime(5, TimeUnit.SECONDS).
                returnDocument(ReturnDocument.AFTER);

        // The found document is in the state AFTER the update

        final var subscriber = new DocumentPrinter("findAndReplace");

        collection.findOneAndReplace(filter, newDocument, options)
                .subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void findAndDelete() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.empty();
        final var sort = Sorts.ascending("_id");
        final var options = new FindOneAndDeleteOptions().sort(sort);

        // The deleted document is returned

        final var subscriber = new DocumentPrinter("findAndDelete");

        collection.findOneAndDelete(filter, options)
                .subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void raceCondition() {
        this.logger.entry();

        // Either John or Jane will get the room

        this.startRace(
                () -> this.bookRoomWithRaceCondition("John"),
                () -> this.bookRoomWithRaceCondition("Jane")
        );

        this.logger.exit();
    }

    private void resetRaceCondition() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("_id", 3);
        final var update = Updates.combine(Updates.set("reserved", false), Updates.set("guest", null));

        final ObservableSubscriber<UpdateResult> subscriber = new OperationSubscriber<>();

        collection.updateOne(filter, update).subscribe(subscriber);

        subscriber.await();

        this.logger.info("{} row(s) were reset between race condition operations", subscriber.first().getModifiedCount());

        this.logger.exit();
    }

    private void noRaceCondition() {
        this.logger.entry();

        // Only Susie will get the room

        this.startRace(
                () -> this.bookRoomWithoutRaceCondition("Susie"),
                () -> this.bookRoomWithoutRaceCondition("Laura")
        );

        this.logger.exit();
    }

    private void startRace(final Runnable runnable1, final Runnable runnable2) {
        this.logger.entry(runnable1, runnable2);

        final var t1 = new Thread(runnable1);
        final var t2 = new Thread(runnable2);

        t1.start();
        t2.start();

        try {
            t1.join();
            t2.join();
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        this.logger.exit();
    }

    private void bookRoomWithRaceCondition(final String name) {
        this.logger.entry(name);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("reserved", false);

        final ObservableSubscriber<Document> findSubscriber = new OperationSubscriber<>();

        collection.find(filter).first().subscribe(findSubscriber);

        findSubscriber.await();

        final var room = findSubscriber.first();

        if (room == null) {
            this.logger.warn("Sorry, {}, a room is not available", name);
        } else {
            this.logger.info("Congratulations, {}, a room is available", name);

            final var update = Updates.combine(Updates.set("reserved", true), Updates.set("guest", name));
            final var roomFilter = Filters.eq("_id", room.get("_id", Integer.class));

            final ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();

            collection.updateOne(roomFilter, update).subscribe(updateSubscriber);

            updateSubscriber.await();
        }

        this.logger.exit();
    }

    private void bookRoomWithoutRaceCondition(final String name) {
        this.logger.entry(name);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("reserved", false);
        final var update = Updates.combine(Updates.set("reserved", true), Updates.set("guest", name));

        final ObservableSubscriber<Document> findSubscriber = new OperationSubscriber<>();

        collection.findOneAndUpdate(filter, update).subscribe(findSubscriber);

        findSubscriber.await();

        final var room = findSubscriber.first();

        if (room == null) {
            this.logger.warn("Sorry, {}, a room is not available", name);
        } else {
            this.logger.info("Congratulations, {}, a room is available", name);
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
            this.logger.error(subscriber.getError().getMessage());
        }

        this.logger.exit();
    }
}
