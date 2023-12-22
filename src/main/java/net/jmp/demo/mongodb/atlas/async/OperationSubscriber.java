package net.jmp.demo.mongodb.atlas.async;

/*
 * (#)OperationSubscriber.java  0.2.0   12/20/2023
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
 * @since     0.2.0
 */

final class OperationSubscriber<T> extends ObservableSubscriber<T> {
    OperationSubscriber() {
        super();
    }
}
