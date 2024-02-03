package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Bulk.java 0.7.0   01/12/2024
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

import com.mongodb.client.model.*;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.bson.Document;

import org.bson.conversions.Bson;
import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Query {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    private final Bson projectionFields = Projections.fields(
            Projections.include("color", "qty", "vendor", "rating"),
            Projections.excludeId());

    class DocumentPrinter extends ConsumerSubscriber<Document> {
        DocumentPrinter(final String methodName) {
            super(document -> {
                if (logger.isInfoEnabled())
                    logger.info("{}: {}", methodName, document.toJson());
            });
        }
    }

    Query(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.query.db", "training");
        this.collectionName = properties.getProperty("mongodb.query.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning query operations...");

        this.insertData();

        this.comparisonOperators();
        this.logicalOperators();
        this.arrayOperators();
        this.elementOperators();
        this.evaluationOperators();

        this.deleteData();

        this.logger.info("Ending query operations...");
        this.logger.exit();
    }

    private void insertData() {
        this.logger.entry();

        final var jsonDocuments = List.of(
                "{ \"_id\": 1, \"color\": \"red\", \"qty\": 9, \"vendor\": [\"A\", \"E\"] }",
                "{ \"_id\": 2, \"color\": \"purple\", \"qty\": 8, \"vendor\": [\"B\", \"D\", \"F\"], \"rating\": 5 }",
                "{ \"_id\": 3, \"color\": \"blue\", \"qty\": 5, \"vendor\": [\"A\", \"E\"] }",
                "{ \"_id\": 4, \"color\": \"white\", \"qty\": 6, \"vendor\": [\"D\"], \"rating\": 9 }",
                "{ \"_id\": 5, \"color\": \"yellow\", \"qty\": 4, \"vendor\": [\"A\", \"B\"] }",
                "{ \"_id\": 6, \"color\": \"pink\", \"qty\": 3, \"vendor\": [\"C\"] }",
                "{ \"_id\": 7, \"color\": \"green\", \"qty\": 8, \"vendor\": [\"C\", \"E\"], \"rating\": 7 }",
                "{ \"_id\": 8, \"color\": \"black\", \"qty\": 7, \"vendor\": [\"A\", \"C\", \"D\"] }"
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

    private void comparisonOperators() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.gt("qty", 7);

        final var sort = Sorts.orderBy(
                Sorts.descending("qty"),
                Sorts.ascending("color")
        );

        final var subscriber = new DocumentPrinter("comparisonOperators");

        collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(sort)
                .subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void logicalOperators() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var filter = Filters.and(
                Filters.lte("qty", 5),
                Filters.ne("color", "pink")
        );

        final var sort = Sorts.orderBy(
                Sorts.descending("qty"),
                Sorts.ascending("color")
        );

        final var subscriber = new DocumentPrinter("logicalOperators");

        collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(sort)
                .subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void arrayOperators() {
        this.logger.entry();

        this.arraySizeOperator();
        this.arrayValueOperator();

        this.logger.exit();
    }

    private void arraySizeOperator() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.size("vendor", 3);   // 3 elements in the array

        final var subscriber = new DocumentPrinter("arraySizeOperator");

        collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(Sorts.ascending("color"))
                .subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void arrayValueOperator() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.eq("vendor", "A");

        final var subscriber = new DocumentPrinter("arrayValueOperator");

        collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(Sorts.ascending("color"))
                .subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void elementOperators() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.exists("rating");

        final var subscriber = new DocumentPrinter("elementOperators");

        collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(Sorts.ascending("rating"))
                .subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void evaluationOperators() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var filter = Filters.regex("color", "k$");

        final var subscriber = new DocumentPrinter("evaluationOperators");

        collection
                .find(filter)
                .projection(this.projectionFields)
                .sort(Sorts.ascending("color"))
                .subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

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
