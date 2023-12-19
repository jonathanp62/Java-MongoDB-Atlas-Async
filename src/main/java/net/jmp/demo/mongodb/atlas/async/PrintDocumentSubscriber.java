package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)PrintDocumentSubscriber.java  0.1.0   12/19/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
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
