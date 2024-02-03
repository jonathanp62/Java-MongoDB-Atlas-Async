package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Collections.java  0.7.0   01/09/2024
 * (#)Collections.java  0.2.0   12/20/2023
 *
 * @author    Jonathan Parker
 * @version   0.7.0
 * @since     0.2.0
 *
 * MIT License
 *
 * Copyright (c) 2023, 2024 Jonathan M. Parker
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
import com.mongodb.reactivestreams.client.MongoDatabase;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Collections {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;

    Collections(final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;
    }

    void ensureCollection(final String dbName, final String collectionName) {
        this.logger.entry(dbName, collectionName);

        if (!this.existsCollection(dbName, collectionName))
            this.createCollection(dbName, collectionName);

        this.logger.exit();
    }

    boolean existsCollection(final String dbName, final String collectionName) {
        this.logger.entry(dbName, collectionName);

        boolean result;

        final MongoDatabase database = this.mongoClient.getDatabase(dbName);
        final ObservableSubscriber<String> listSubscriber = new OperationSubscriber<>();

        database.listCollectionNames()
                .subscribe(listSubscriber);

        listSubscriber.await();

        if (listSubscriber.getError() == null) {
            final var collectionNames = listSubscriber.getReceived();

            if (collectionNames.contains(collectionName)) {
                result = true;

                this.logger.info("Collection {} exists in database {}", collectionName, dbName);
            } else {
                result = false;

                this.logger.warn("Collection {} does not exist in database {}", collectionName, dbName);
            }
        } else {
            final var error = listSubscriber.getError();

            this.logger.error(error.getMessage());

            throw new RuntimeException("Exception checking for collection " + collectionName, error);
        }

        this.logger.exit(result);

        return result;
    }

    private void createCollection(final String dbName, final String collectionName) {
        this.logger.entry(dbName, collectionName);

        final MongoDatabase database = this.mongoClient.getDatabase(dbName);
        final ObservableSubscriber<Void> voidSubscriber = new OperationSubscriber<>();

        database.createCollection(collectionName)
                .subscribe(voidSubscriber);

        voidSubscriber.await();

        if (voidSubscriber.getError() != null) {
            this.logger.error(voidSubscriber.getError().getMessage());
        } else {
            this.logger.info("Collection {} created in database {}", collectionName, dbName);
        }

        this.logger.exit();
    }

    void dropCollection(final String dbName, final String collectionName) {
        this.logger.entry(dbName, collectionName);

        final var database = this.mongoClient.getDatabase(dbName);
        final var collection = database.getCollection(collectionName);

        final ObservableSubscriber<Void> voidSubscriber = new OperationSubscriber<>();

        collection.drop()
                .subscribe(voidSubscriber);

        voidSubscriber.await();

        if (voidSubscriber.getError() != null) {
            this.logger.error(voidSubscriber.getError().getMessage());
        } else {
            this.logger.info("Collection {} dropped from database {}", collectionName, dbName);
        }

        this.logger.exit();
    }
}
