/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.internal;

import org.junit.Test;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.TestUtil;
import reactor.core.test.TestSubscriber;

import static org.junit.Assert.assertTrue;

public class UnicastSubjectTest {

	@Test
	public void testSubscribeReceiveValue() {
		Frame f = TestUtil.utf8EncodedResponseFrame(1, FrameType.NEXT_COMPLETE, "response");
		UnicastSubject<Frame> us = UnicastSubject.create();
		TestSubscriber<Frame> ts = TestSubscriber.create();
		us.subscribe(ts);
		us.onNext(f);
		ts.assertValues(f);
		ts.assertNotTerminated();
	}

	@Test(expected = NullPointerException.class)
	public void testNullPointerSendingWithoutSubscriber() {
		Frame f = TestUtil.utf8EncodedResponseFrame(1, FrameType.NEXT_COMPLETE, "response");
		UnicastSubject<Frame> us = UnicastSubject.create();
		us.onNext(f);
	}

	@Test
	public void testIllegalStateIfMultiSubscribe() {
		UnicastSubject<Frame> us = UnicastSubject.create();
		TestSubscriber<Frame> f1 = TestSubscriber.create();
		us.subscribe(f1);
		TestSubscriber<Frame> f2 = TestSubscriber.create();
		us.subscribe(f2);

		f1.assertNotTerminated();
		f2.assertErrorWith(e ->
			assertTrue(IllegalStateException.class.isInstance(e) || NullPointerException.class.isInstance(e))
		);
	}

}
