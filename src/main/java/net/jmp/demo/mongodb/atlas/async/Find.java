package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Find.java 0.1.0   12/16/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.1.0
 * @since     0.1.0
 */

import com.mongodb.reactivestreams.client.MongoClient;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import java.util.Properties;

import java.util.concurrent.CountDownLatch;

import org.bson.Document;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

import static com.mongodb.client.model.Filters.eq;

final class Find {
    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));
    private final MongoClient mongoClient;
    private final String dbName;
    private final String collectionName;

    Find(final Properties properties, final MongoClient mongoClient) {
        super();

        this.mongoClient = mongoClient;

        this.dbName = properties.getProperty("mongodb.find.db", "sample_mflix");
        this.collectionName = properties.getProperty("mongodb.find.collection", "movies");
    }

    void run() {
        this.logger.entry();
        this.logger.info("Beginning find operations...");

        this.findOneDocument();
        this.findMultipleDocuments();

        this.logger.info("Ending find operations.");
        this.logger.exit();
    }

    private void findOneDocument() {
        this.logger.entry();

        final var latch = new CountDownLatch(1);
        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var projectionFields = Projections.fields(
                Projections.include("title", "imdb"),
                Projections.excludeId());

        collection.find(eq("title", "The Room"))
                .projection(projectionFields)
                .sort(Sorts.descending("imdb.rating"))
                .first()
                .subscribe(new MySubscriber(latch));

        try {
            latch.await();  // Can provide a timeout
        } catch (final InterruptedException ie) {
            this.logger.catching(ie);

            Thread.currentThread().interrupt();
        }

        this.logger.exit();
    }

    private void findMultipleDocuments() {
        this.logger.entry();
        this.logger.exit();
    }

    class MySubscriber implements Subscriber<Document> {
        private final CountDownLatch latch;

        MySubscriber(final CountDownLatch latch) {
            super();

            this.latch = latch;
        }

        @Override
        public void onSubscribe(final Subscription subscription) {
            subscription.request(1); // Number of elements to request of the publisher
        }

        @Override
        public void onNext(final Document document) {
            if (logger.isInfoEnabled())
                logger.info(document.toJson());
        }

        @Override
        public void onError(final Throwable throwable) {
            logger.error(throwable.getMessage());
        }

        @Override
        public void onComplete() {
            this.latch.countDown();
        }
    }
}
