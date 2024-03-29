package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Insert.java   0.7.0   01/09/2024
 * (#)Insert.java   0.2.0   12/20/2023
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

import com.mongodb.MongoBulkWriteException;

import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;

import com.mongodb.reactivestreams.client.MongoClient;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.bson.Document;

import org.bson.types.ObjectId;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

final class Insert {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Insert(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.insert.db", "training");
        this.collectionName = properties.getProperty("mongodb.insert.collection", "colors");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning insert operations...");

        final var collections = new Collections(this.mongoClient);

        collections.ensureCollection(this.dbName, this.collectionName);

        this.insertOneDocument();
        this.insertMultipleDocuments();

        Helpers.printAllDocuments(this.mongoClient,
                this.dbName,
                this.collectionName,
                this.logger);

        this.logger.info("Ending insert operations.");
        this.logger.exit();
    }

    private void insertOneDocument() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);
        final var document = new Document("color", "red").append("qty", 5);

        ObservableSubscriber<InsertOneResult> insertSubscriber = new OperationSubscriber<>();

        collection.insertOne(document).subscribe(insertSubscriber);
        insertSubscriber.await();

        if (insertSubscriber.getError() == null) {
            final var result = insertSubscriber.get().getFirst();

            this.logger.info("Inserted document: {}", Objects.requireNonNull(result.getInsertedId()).asObjectId().getValue());
        } else {
            this.logger.error(insertSubscriber.getError().getMessage());
        }

        insertSubscriber = new OperationSubscriber<>(); // Don't reuse a subscriber

        collection.insertOne(new Document()
                .append("_id", new ObjectId())
                .append("color", "orange")
                .append("qty", 6)
                    )
                .subscribe(insertSubscriber);
        insertSubscriber.await();

        if (insertSubscriber.getError() == null) {
            final var result = insertSubscriber.get().getFirst();

            this.logger.info("Inserted document: {}", Objects.requireNonNull(result.getInsertedId()).asObjectId().getValue());
        } else {
            this.logger.error(insertSubscriber.getError().getMessage());
        }

        this.logger.exit();
    }

    private void insertMultipleDocuments() {
        this.logger.entry();

        final var color = "color";
        final var quantity = "qty";
        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var documents = List.of(
                new Document(color, "blue").append(quantity, 5),
                new Document(color, "purple").append(quantity, 8),
                new Document(color, "green").append(quantity, 9),
                new Document(color, "yellow").append(quantity, 5)
        );

        final ObservableSubscriber<InsertManyResult> insertSubscriber = new OperationSubscriber<>();

        collection.insertMany(documents).subscribe(insertSubscriber);
        insertSubscriber.await();

        if (insertSubscriber.getError() == null) {
            final var result = insertSubscriber.get().getFirst();

            result.getInsertedIds().values()
                    .forEach(id -> this.logger.info("Inserted document: {}", id.asObjectId().getValue()));
        } else {
            final var error = insertSubscriber.getError();

            this.logger.error(error.getMessage());

            if (error instanceof MongoBulkWriteException mbwe) {
                mbwe.getWriteResult().getInserts()
                        .forEach(doc -> this.logger.info("Inserted document: {}", doc.getId().asObjectId().getValue()));
            }
        }

        this.logger.exit();
    }
}
