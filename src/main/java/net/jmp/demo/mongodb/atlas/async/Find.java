package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Find.java 0.2.0   12/20/2023
 * (#)Find.java 0.1.0   12/16/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.2.0
 * @since     0.1.0
 */

import com.mongodb.reactivestreams.client.MongoClient;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import java.util.Properties;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;

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

        final var collections = new Collections(this.mongoClient);

        if (collections.existsCollection(this.dbName, this.collectionName)) {
            this.findOneDocument();
            this.findMultipleDocuments();
        }

        this.logger.info("Ending find operations.");
        this.logger.exit();
    }

    private void findOneDocument() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var projectionFields = Projections.fields(
                Projections.include("title", "imdb"),
                Projections.excludeId());

        final var documentSubscriber = new PrintDocumentSubscriber(this.logger);

        collection.find(eq("title", "The Room"))
            .projection(projectionFields)
            .sort(Sorts.descending("imdb.rating"))
            .first()
            .subscribe(documentSubscriber);

        documentSubscriber.await();

        this.logger.exit();
    }

    private void findMultipleDocuments() {
        this.logger.entry();

        final var database = this.mongoClient.getDatabase(this.dbName);
        final var collection = database.getCollection(this.collectionName);

        final var projectionFields = Projections.fields(
                Projections.include("title", "runtime", "imdb"),
                Projections.excludeId());

        final var documentSubscriber = new PrintDocumentSubscriber(this.logger);

        collection
            .find(lt("runtime", 15))
            .projection(projectionFields)
            .sort(Sorts.descending("title"))
            .subscribe(documentSubscriber);

        documentSubscriber.await();

        final var numberOfResults = documentSubscriber.getReceived().size();

        this.logger.info("There are {} results available", numberOfResults);

        this.logger.exit();
    }
}
