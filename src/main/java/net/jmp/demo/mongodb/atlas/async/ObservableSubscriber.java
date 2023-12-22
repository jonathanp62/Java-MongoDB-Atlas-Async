package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)ObservableSubscriber.java 0.2.0   12/20/2023
 * (#)ObservableSubscriber.java 0.1.0   12/19/2023
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
 * @version   0.2.0
 * @since     0.1.0
 */

import com.mongodb.MongoTimeoutException;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;

abstract class ObservableSubscriber<T> implements Subscriber<T> {
    private final List<T> received;
    private final List<RuntimeException> errors;
    private final CountDownLatch latch;
    private volatile Subscription subscription;
    private volatile boolean completed;

    ObservableSubscriber() {
        super();

        this.received = new ArrayList<>();
        this.errors = new ArrayList<>();
        this.latch = new CountDownLatch(1);
    }

    @Override
    public void onSubscribe(final Subscription s) {
        this.subscription = s;

        this.subscription.request(Integer.MAX_VALUE);
    }

    @Override
    public void onNext(final T t) {
        this.received.add(t);
    }

    @Override
    public void onError(final Throwable t) {
        if (t instanceof RuntimeException runtimeException) {
            this.errors.add(runtimeException);
        } else {
            this.errors.add(new RuntimeException("Unexpected exception", t));
        }

        this.onComplete();
    }

    @Override
    public void onComplete() {
        this.completed = true;

        this.latch.countDown();
    }

    Subscription getSubscription() {
        return this.subscription;
    }

    List<T> getReceived() {
        return this.received;
    }

    RuntimeException getError() {
        if (!this.errors.isEmpty()) {
            return this.errors.get(0);
        }

        return null;
    }

    List<T> get() {
        return this.await().getReceived();
    }

    List<T> get(final long timeout, final TimeUnit unit) {
        return this.await(timeout, unit).getReceived();
    }

    public T first() {
        final var receivedElements = this.await().getReceived();

        return !receivedElements.isEmpty() ? receivedElements.get(0) : null;
    }

    ObservableSubscriber<T> await() {
        return this.await(60, TimeUnit.SECONDS);
    }

    ObservableSubscriber<T> await(final long timeout, final TimeUnit unit) {
        try {
            if (!this.latch.await(timeout, unit)) {
                throw new MongoTimeoutException("Publisher onComplete timed out");
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();

            throw interruptAndCreateMongoInterruptedException("Interrupted waiting for observeration", ie);
        }

        if (!this.errors.isEmpty()) {
            throw this.errors.get(0);
        }

        return this;
    }
}
