package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Delete.java   0.7.0   01/09/2024
 * (#)Delete.java   0.4.0   01/02/2024
 *
 * @author    Jonathan Parker
 * @version   0.7.0
 * @since     0.4.0
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

        if (deleteSubscriber.getError() == null)
            this.logger.info("{} document(s) were deleted", deleteSubscriber.first().getDeletedCount());
        else
            this.logger.error(deleteSubscriber.getError().getMessage());

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

        if (deleteSubscriber.getError() == null) {
            final var document = deleteSubscriber.first();

            if (document != null) {
                if (this.logger.isInfoEnabled())
                    this.logger.info("Deleted document: {}", document.toJson());
            } else {
                this.logger.warn("No documents with a color of orange were found");
            }
        } else {
            this.logger.error(deleteSubscriber.getError().getMessage());
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

        if (deleteSubscriber.getError() == null)
            this.logger.info("{} document(s) were deleted", deleteSubscriber.first().getDeletedCount());
        else
            this.logger.error(deleteSubscriber.getError().getMessage());

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

        if (deleteSubscriber.getError() == null)
            this.logger.info("{} document(s) were deleted", deleteSubscriber.first().getDeletedCount());
        else
            this.logger.error(deleteSubscriber.getError().getMessage());

        this.logger.exit();
    }
}
