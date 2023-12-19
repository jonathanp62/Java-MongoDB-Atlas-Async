package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)ConsumerSubscriber.java   0.1.0   12/19/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.1.0
 * @since     0.1.0
 */

import java.util.function.Consumer;

class ConsumerSubscriber<T> extends AbstractObservableSubscriber<T> {
    private Consumer<T> consumer;

    ConsumerSubscriber(final Consumer<T> consumer) {
        super();

        this.consumer = consumer;
    }

    void setConsumer(final Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onNext(final T document) {
        super.onNext(document);

        this.consumer.accept(document);
    }
}
