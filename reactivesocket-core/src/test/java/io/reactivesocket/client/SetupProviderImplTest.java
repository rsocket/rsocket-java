/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.reactivesocket.client;

import io.reactivesocket.Frame;
import io.reactivesocket.Frame.Setup;
import io.reactivesocket.FrameType;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.lease.DefaultLeaseEnforcingSocket;
import io.reactivesocket.lease.DefaultLeaseHonoringSocket;
import io.reactivesocket.lease.FairLeaseDistributor;
import io.reactivesocket.test.util.TestDuplexConnection;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import org.junit.Test;

import static io.reactivesocket.client.SetupProvider.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class SetupProviderImplTest {

    @Test(timeout = 2000)
    public void testSetup() throws Exception {
        Frame setup = Setup.from(0, 0, 0, DEFAULT_DATA_MIME_TYPE, DEFAULT_DATA_MIME_TYPE, PayloadImpl.EMPTY);
        SetupProviderImpl setupProvider =
                new SetupProviderImpl(setup, reactiveSocket -> new DefaultLeaseHonoringSocket(reactiveSocket),
                                      KeepAliveProvider.never(), Throwable::printStackTrace);
        TestDuplexConnection connection = new TestDuplexConnection();
        FairLeaseDistributor distributor = new FairLeaseDistributor(() -> 0, 0, Flowable.never());
        ReactiveSocket socket = Flowable.fromPublisher(setupProvider
                                                 .accept(connection,
                                                         reactiveSocket -> new DefaultLeaseEnforcingSocket(
                                                                 reactiveSocket, distributor)))
                                      .switchIfEmpty(Flowable.error(new IllegalStateException("No socket returned.")))
                                      .blockingFirst();
        assertThat("Unexpected socket.", socket, is(notNullValue()));
        assertThat("Unexpected frames sent on connection.", connection.getSent(), hasSize(1));
        assertThat("Unexpected frame sent on connection.", connection.getSent().iterator().next().getType(),
                   is(FrameType.SETUP));
    }
}