package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)ConsumerSubscriber.java   0.1.0   12/19/2023
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
 *
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
