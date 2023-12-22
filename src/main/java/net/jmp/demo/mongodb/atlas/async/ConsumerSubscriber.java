package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)ConsumerSubscriber.java   0.1.0   12/19/2023
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

import java.util.function.Consumer;

class ConsumerSubscriber<T> extends ObservableSubscriber<T> {
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
