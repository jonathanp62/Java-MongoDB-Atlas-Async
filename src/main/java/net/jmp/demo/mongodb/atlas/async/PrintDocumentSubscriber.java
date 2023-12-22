package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)PrintDocumentSubscriber.java  0.1.0   12/19/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * For the purposes of learning and developing this
 * demonstration program, this class borrows heavily
 * from and acknowledges the following cited source
 * module as developed by MongoDB, Inc.:
 *
 * https://github.com/mongodb/mongo-java-driver/blob/master/driver-reactive-streams/src/examples/reactivestreams/helpers/SubscriberHelpers.java
 *
 * @author    Jonathan Parker
 * @version   0.1.0
 * @since     0.1.0
 */

import org.bson.Document;

import org.slf4j.ext.XLogger;

public class PrintDocumentSubscriber extends ConsumerSubscriber<Document> {
    public PrintDocumentSubscriber(final XLogger logger) {
        super((document -> logger.info(document.toJson())));
    }
}
