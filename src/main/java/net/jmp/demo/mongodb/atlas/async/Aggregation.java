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

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Aggregation {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

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
