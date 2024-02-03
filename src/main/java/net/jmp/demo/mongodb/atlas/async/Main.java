package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)Main.java 0.9.0   02/03/2024
 * (#)Main.java 0.8.0   02/03/2024
 * (#)Main.java 0.7.0   01/09/2024
 * (#)Main.java 0.6.0   01/09/2024
 * (#)Main.java 0.5.0   01/08/2024
 * (#)Main.java 0.4.0   01/02/2024
 * (#)Main.java 0.3.0   12/21/2023
 * (#)Main.java 0.2.0   12/20/2023
 * (#)Main.java 0.1.0   12/15/2023
 *
 * @author    Jonathan Parker
 * @version   0.9.0
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

import com.mongodb.reactivestreams.client.MongoClients;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.Optional;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import org.slf4j.ext.XLogger;

public final class Main {
    private static final String MONGODB_URI = "mongodb.uri";
    private static final String MONGODB_URI_LOGGABLE = "mongodb.uri.loggable";

    private final XLogger logger = new XLogger(LoggerFactory.getLogger(this.getClass().getName()));

    private Main() {
        super();
    }

    private void run() {
        this.logger.entry();
        this.logger.info("Beginning MongoDb Atlas async demo...");

        final var properties = this.getAppProperties();

        properties.ifPresent(props -> {
            final var mongoDbUri = props.getProperty(MONGODB_URI);
            final var mongoDbUriLoggable = props.getProperty(MONGODB_URI_LOGGABLE);

            this.logger.info("Connecting to {}", mongoDbUriLoggable);

            try (final var mongoClient = MongoClients.create(mongoDbUri)) {
                new Find(props, mongoClient).run();
                new Insert(props, mongoClient).run();
                new UpdateAndReplace(props, mongoClient).run();
                new Delete(props, mongoClient).run();
                new UpdateArrays(props, mongoClient).run();
                new Upsert(props, mongoClient).run();
                new Bulk(props, mongoClient).run();
                new Query(props, mongoClient).run();
                new Compound(props, mongoClient).run();
                new Aggregation(props, mongoClient).run();
            } finally {
                this.logger.info("Disconnected from {}", mongoDbUriLoggable);
            }
        });

        this.logger.info("Ending MongoDb Atlas async demo.");
        this.logger.exit();
    }

    private Optional<Properties> getAppProperties() {
        this.logger.entry();

        Properties properties = null;

        final var configFileName = System.getProperty("app.configurationFile");

        if (configFileName != null) {
            this.logger.info("Loading the configuration from {}", configFileName);

            properties = new Properties();

            try (final var fis = new FileInputStream(configFileName)) {
                properties.load(fis);

                this.replaceUriWithSecrets(properties);
            } catch (final IOException ioe) {
                this.logger.catching(ioe);
            }
        } else {
            this.logger.error("System property app.configurationFile was not found");
        }

        final var results = Optional.ofNullable(properties);

        this.logger.exit(results);

        return results;
    }

    private void replaceUriWithSecrets(final Properties appProperties) {
        this.logger.entry(appProperties);

        var mongoDbUri = appProperties.getProperty(MONGODB_URI);

        appProperties.setProperty(MONGODB_URI_LOGGABLE, mongoDbUri);

        final var configFileName = System.getProperty("app.configurationFile");
        final var configDirectory = new File(configFileName).getParent();
        final var secretsConfigFileName = configDirectory + File.separator + "secrets.properties";

        this.logger.info("Loading the secrets configuration from {}", secretsConfigFileName);

        final var secretProperties = new Properties();

        try (final var fis = new FileInputStream(secretsConfigFileName)) {
            secretProperties.load(fis);

            mongoDbUri = mongoDbUri.replace("{uri.userid}", secretProperties.getProperty("mongodb.uri.userid"));
            mongoDbUri = mongoDbUri.replace("{uri.password}", secretProperties.getProperty("mongodb.uri.password"));
            mongoDbUri = mongoDbUri.replace("{uri.domain}", secretProperties.getProperty("mongodb.uri.domain"));

            appProperties.setProperty(MONGODB_URI, mongoDbUri);
        } catch (final IOException ioe) {
            this.logger.catching(ioe);
        }

        this.logger.exit();
    }

    public static void main(final String[] args) {
        new Main().run();
    }
}
