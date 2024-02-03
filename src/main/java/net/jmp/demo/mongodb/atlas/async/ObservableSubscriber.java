package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)ObservableSubscriber.java 0.2.0   12/20/2023
 * (#)ObservableSubscriber.java 0.1.0   12/19/2023
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
 *
 * MIT License
 *
 * Copyright (c) 2023 Jonathan M. Parker
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
