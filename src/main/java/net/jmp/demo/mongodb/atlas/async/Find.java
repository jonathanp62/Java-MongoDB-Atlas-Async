package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Find.java 0.7.0   01/09/2024
 * (#)Find.java 0.3.0   12/22/2023
 * (#)Find.java 0.2.0   12/20/2023
 * (#)Find.java 0.1.0   12/16/2023
 *
 * @author    Jonathan Parker
 * @version   0.7.0
 * @since     0.1.0
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

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import com.mongodb.reactivestreams.client.MongoClient;

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

        if (documentSubscriber.getError() != null)
            this.logger.error(documentSubscriber.getError().getMessage());

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

        if (documentSubscriber.getError() == null) {
            final var numberOfResults = documentSubscriber.getReceived().size();

            this.logger.info("There are {} results available", numberOfResults);
        } else {
            this.logger.error(documentSubscriber.getError().getMessage());
        }

        this.logger.exit();
    }
}
