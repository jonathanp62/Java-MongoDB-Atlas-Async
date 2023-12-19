package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)AbstractObservableSubscriber.java 0.1.0   12/19/2023
 *
 * Copyright (c) Jonathan M. Parker
 * All Rights Reserved.
 *
 * @author    Jonathan Parker
 * @version   0.1.0
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

abstract class AbstractObservableSubscriber<T> implements Subscriber<T> {
    private final List<T> received;
    private final List<RuntimeException> errors;
    private final CountDownLatch latch;
    private volatile Subscription subscription;
    private volatile boolean completed;

    AbstractObservableSubscriber() {
        super();

        this.received = new ArrayList<>();
        this.errors = new ArrayList<>();
        this.latch = new CountDownLatch(1);
    }

    @Override
    public void onSubscribe(final Subscription s) {
        this.subscription = s;
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

    AbstractObservableSubscriber<T> await() {
        return this.await(60, TimeUnit.SECONDS);
    }

    AbstractObservableSubscriber<T> await(final long timeout, final TimeUnit unit) {
        this.subscription.request(Integer.MAX_VALUE);

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
