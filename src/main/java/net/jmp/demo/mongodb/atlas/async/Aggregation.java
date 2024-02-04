package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Aggregation.java  0.9.0   02/03/2024
 *
 * @author    Jonathan Parker
 * @version   0.9.0
 * @since     0.9.0
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

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

import com.mongodb.client.result.InsertManyResult;

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.bson.Document;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Aggregation {
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

    Aggregation(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.aggregation.db", "training");
        this.collectionName = properties.getProperty("mongodb.aggregation.collection", "restaurants");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning aggregation operations...");

        this.createCollection();

        try {
            this.insertData();
            this.basic();
            this.explain();
            this.expression();
        } finally {
            this.dropCollection();
        }

        this.logger.info("Ending aggregation operations...");
        this.logger.exit();
    }

    private void createCollection() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);

        final List<String> collections = new ArrayList<>();
        final ConsumerSubscriber<String> consumerSubscriber = new ConsumerSubscriber<>(collections::add);   // string -> collections.add(string)

        database.listCollectionNames().subscribe(consumerSubscriber);

        consumerSubscriber.await();

        if (!collections.contains(this.collectionName)) {
            this.logger.info("Collection {} not found; creating it...", this.collectionName);

            final OperationSubscriber<Void> voidSubscriber = new OperationSubscriber<>();

            database.createCollection(this.collectionName).subscribe(voidSubscriber);

            voidSubscriber.await();

            this.logger.info("Created collection {}", this.collectionName);
        } else {
            this.logger.info("Collection {} found", this.collectionName);
        }

        this.logger.exit();
    }

    private void insertData() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var documents = List.of(
                new Document("name", "Sun Bakery Trattoria").append("contact", new Document().append("phone", "386-555-0189").append("email", "SunBakeryTrattoria@example.org").append("location", Arrays.asList(-74.0056649, 40.7452371))).append("stars", 4).append("categories", Arrays.asList("Pizza", "Pasta", "Italian", "Coffee", "Sandwiches")),
                new Document("name", "Blue Bagels Grill").append("contact", new Document().append("phone", "786-555-0102").append("email", "BlueBagelsGrill@example.com").append("location", Arrays.asList(-73.92506, 40.8275556))).append("stars", 3).append("categories", Arrays.asList("Bagels", "Cookies", "Sandwiches")),
                new Document("name", "XYZ Bagels Restaurant").append("contact", new Document().append("phone", "435-555-0190").append("email", "XYZBagelsRestaurant@example.net").append("location", Arrays.asList(-74.0707363, 40.59321569999999))).append("stars", 4).append("categories", Arrays.asList("Bagels", "Sandwiches", "Coffee")),
                new Document("name", "Hot Bakery Cafe").append("contact", new Document().append("phone", "264-555-0171").append("email", "HotBakeryCafe@example.net").append("location", Arrays.asList(-73.96485799999999, 40.761899))).append("stars", 4).append("categories", Arrays.asList("Bakery", "Cafe", "Coffee", "Dessert")),
                new Document("name", "Green Feast Pizzeria").append("contact", new Document().append("phone", "840-555-0102").append("email", "GreenFeastPizzeria@example.com").append("location", Arrays.asList(-74.1220973, 40.6129407))).append("stars", 2).append("categories", Arrays.asList("Pizza", "Italian")),
                new Document("name", "ZZZ Pasta Buffet").append("contact", new Document().append("phone", "769-555-0152").append("email", "ZZZPastaBuffet@example.com").append("location", Arrays.asList(-73.9446421, 40.7253944))).append("stars", 0).append("categories", Arrays.asList("Pasta", "Italian", "Buffet", "Cafeteria")),
                new Document("name", "XYZ Coffee Bar").append("contact", new Document().append("phone", "644-555-0193").append("email", "XYZCoffeeBar@example.net").append("location", Arrays.asList(-74.0166091, 40.6284767))).append("stars", 5).append("categories", Arrays.asList("Coffee", "Cafe", "Bakery", "Chocolates")),
                new Document("name", "456 Steak Restaurant").append("contact", new Document().append("phone", "990-555-0165").append("email", "456SteakRestaurant@example.com").append("location", Arrays.asList(-73.9365108, 40.8497077))).append("stars", 0).append("categories", Arrays.asList("Steak", "Seafood")),
                new Document("name", "456 Cookies Shop").append("contact", new Document().append("phone", "604-555-0149").append("email", "456CookiesShop@example.org").append("location", Arrays.asList(-73.8850023, 40.7494272))).append("stars", 4).append("categories", Arrays.asList("Bakery", "Cookies", "Cake", "Coffee")),
                new Document("name", "XYZ Steak Buffet").append("contact", new Document().append("phone", "229-555-0197").append("email", "XYZSteakBuffet@example.org").append("location", Arrays.asList(-73.9799932, 40.7660886))).append("stars", 3).append("categories", Arrays.asList("Steak", "Salad", "Chinese"))
        );

        final ObservableSubscriber<InsertManyResult> subscriber = new OperationSubscriber<>();

        collection.insertMany(documents).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() == null) {
            subscriber.first().getInsertedIds().values()
                    .forEach(id -> this.logger.info("Inserted document: {}", id));
        } else {
            final var error = subscriber.getError();

            if (error instanceof MongoBulkWriteException mbwe) {
                mbwe.getWriteResult().getInserts()
                        .forEach(doc -> this.logger.info("Inserted document: {}", doc.getId()));
            }

            this.logger.error(error.getMessage());
        }

        this.logger.exit();
    }

    private void basic() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var subscriber = new DocumentPrinter("basic");

        collection.aggregate(
                Arrays.asList(
                        Aggregates.match(Filters.eq("categories", "Bakery")),
                        Aggregates.group("$stars", Accumulators.sum("count", 1))
                )
        ).subscribe(subscriber);

        subscriber.await();

        /*
         * {"_id": 4, "count": 2} // Two four-star bakeries
         * {"_id": 5, "count": 1} // One five-star bakery
         */

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void explain() {
        this.logger.entry();

//        final var database = this.mongoClient.getDatabase(this.dbName);
//        final var collection = database.getCollection(this.collectionName);
//
//        final ObservableSubscriber<Document> subscriber = new OperationSubscriber<>();
//
//        collection.aggregate(
//                Arrays.asList(
//                        Aggregates.match(Filters.eq("categories", "Bakery")),
//                        Aggregates.group("$stars", Accumulators.sum("count", 1))
//                )
//        ).subscribe(subscriber);
//
//        subscriber.await();
//
//        final Document explanation = subscriber.first().explain(ExplainVerbosity.EXECUTION_STATS);
//
//        @SuppressWarnings("unchecked")
//        final List<Document> stages = explanation.get("stages", List.class);
//
//        if (stages != null) {
//            final List<String> keys = Arrays.asList("queryPlanner", "winningPlan");
//
//            for (final var stage : stages) {
//                final var cursorStage = stage.get("$cursor", Document.class);
//
//                if (cursorStage != null && (this.logger.isInfoEnabled()))
//                    this.logger.info(cursorStage.getEmbedded(keys, Document.class).toJson());
//            }
//        } else {
//            /* Provided as the stages variable seems to always be null */
//
//            Helpers.printOneDocument(explanation, this.logger);
//        }

        this.logger.exit();
    }

    private void expression() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var subscriber = new DocumentPrinter("expression");

        collection.aggregate(
                List.of(
                        Aggregates.project(
                                Projections.fields(
                                        Projections.excludeId(),
                                        Projections.include("name"),
                                        Projections.computed(
                                                "firstCategory",
                                                new Document("$arrayElemAt", Arrays.asList("$categories", 0))
                                        )
                                )
                        )
                )
        ).subscribe(subscriber);

        subscriber.await();

        if (subscriber.getError() != null)
            this.logger.error(subscriber.getError().getMessage());

        this.logger.exit();
    }

    private void dropCollection() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final OperationSubscriber<Void> voidSubscriber = new OperationSubscriber<>();

        collection.drop().subscribe(voidSubscriber);

        voidSubscriber.await();

        this.logger.info("Dropped collection {}", this.collectionName);

        this.logger.exit();
    }
}
