package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)UpdateArrays.java 0.7.0   01/09/2024
 * (#)UpdateArrays.java 0.5.0   01/08/2024
 *
 * @author    Jonathan Parker
 * @version   0.7.0
 * @since     0.5.0
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

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.bson.Document;

import org.bson.types.ObjectId;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class UpdateArrays {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    UpdateArrays(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.update.db", "training");
        this.collectionName = properties.getProperty("mongodb.update.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning update arrays operations...");

        final var objectId = this.insertData();

        if (objectId.isPresent()) {
            final var id = objectId.get();

            this.appendQuantity(id);
            this.printOneDocument(id);

            this.updateFirstArrayElement(id);
            this.printOneDocument(id);

            this.updateAllArrayElements(id);
            this.printOneDocument(id);

            this.updateMultipleArrayElements(id);
            this.printOneDocument(id);

            this.deleteData(id);
        }

        this.logger.info("Ending update arrays operations...");
        this.logger.exit();
    }

    private Optional<ObjectId> insertData() {
        this.logger.entry();

        ObjectId objectId = null;

        final List<Integer> quantities = List.of(
                8, 12, 18
        );

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var document = new Document("color", "green").append("quantities", quantities);

        final ObservableSubscriber<InsertOneResult> subscriber = new OperationSubscriber<>();

        collection.insertOne(document).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            objectId = Objects.requireNonNull(subscriber.first().getInsertedId()).asObjectId().getValue();

            this.logger.info("Inserted document: {}", objectId);
        } else {
            this.logger.error(subscriber.getError().getMessage());
        }

        final var result = Optional.ofNullable(objectId);

        this.logger.exit(result);

        return result;
    }

    private void appendQuantity(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("_id", objectId);
        final var update = Updates.push("quantities", 26);

        final var options = new FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER);

        final ObservableSubscriber<Document> subscriber = new OperationSubscriber<>();

        collection.findOneAndUpdate(filter, update, options).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            final var document = subscriber.first();

            if (document != null) {
                @SuppressWarnings("unchecked") final List<Integer> quantities = document.get("quantities", List.class);

                if (quantities != null)
                    quantities.forEach(q -> this.logger.info("Quantity: {}", q));
            }
        } else {
            this.logger.error(subscriber.getError().getMessage());
        }

        this.logger.exit();
    }

    private void updateFirstArrayElement(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var filter = Filters.and(
                Filters.eq("_id", objectId),
                Filters.eq("quantities", 18)
        );

        final var update = Updates.inc("quantities.$", -3);

        final var options = new FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER);

        final ObservableSubscriber<Document> subscriber = new OperationSubscriber<>();

        collection.findOneAndUpdate(filter, update, options).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            final var document = subscriber.first();

            if (document != null)
                this.logger.info("Updated the {} document", document.get("color"));

        } else {
            this.logger.error(subscriber.getError().getMessage());
        }

        this.logger.exit();
    }

    private void updateAllArrayElements(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("_id", objectId);
        final var update = Updates.mul("quantities.$[]", 2);

        final var options = new FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER);

        final ObservableSubscriber<Document> subscriber = new OperationSubscriber<>();

        collection.findOneAndUpdate(filter, update, options).subscribe(subscriber);

        if (subscriber.getError() == null) {
            final var document = subscriber.first();

            if (document != null)
                this.logger.info("Updated the {} document", document.get("color"));
        } else {
            this.logger.error(subscriber.getError().getMessage());
        }

        this.logger.exit();
    }

    private void updateMultipleArrayElements(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("_id", objectId);
        final var smallerFilter = Filters.lt("smaller", 30);
        final var update = Updates.inc("quantities.$[smaller]", 5);

        final var options = new FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
                .arrayFilters(List.of(smallerFilter));

        final ObservableSubscriber<Document> subscriber = new OperationSubscriber<>();

        collection.findOneAndUpdate(filter, update, options).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            final var document = subscriber.first();

            if (document != null)
                this.logger.info("Updated the {} document", document.get("color"));
        } else {
            this.logger.error(subscriber.getError().getMessage());
        }

        this.logger.exit();
    }

    private void deleteData(final ObjectId objectId) {
        this.logger.entry(objectId);

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("_id", objectId);

        final ObservableSubscriber<DeleteResult> subscriber = new OperationSubscriber<>();

        collection.deleteOne(filter).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null)
            this.logger.info("{} document(s) were deleted", subscriber.first().getDeletedCount());
        else
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void printOneDocument(final ObjectId objectId) {
        this.logger.entry(objectId);

        Helpers.printOneDocument(this.mongoClient,
                this.dbName,
                this.collectionName,
                Filters.eq("_id", objectId),
                this.logger);

        this.logger.exit();
    }
}
