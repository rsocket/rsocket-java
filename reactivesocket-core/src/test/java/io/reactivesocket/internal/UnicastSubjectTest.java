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
import rx.observers.TestSubscriber;

import static io.reactivesocket.TestUtil.testSubscribe;
import static org.junit.Assert.assertTrue;

public class UnicastSubjectTest {

	@Test
	public void testSubscribeReceiveValue() {
		Frame f = TestUtil.utf8EncodedResponseFrame(1, FrameType.NEXT_COMPLETE, "response");
		UnicastSubject<Frame> us = UnicastSubject.create();
		TestSubscriber<Frame> ts = testSubscribe(us);
		us.onNext(f);
		ts.assertValue(f);
		ts.assertNoTerminalEvent();
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
		TestSubscriber<Frame> f1 = testSubscribe(us);
		TestSubscriber<Frame> f2 = testSubscribe(us);

		f1.assertNoTerminalEvent();
		for (Throwable e : f2.getOnErrorEvents()) {
			assertTrue( IllegalStateException.class.isInstance(e) || NullPointerException.class.isInstance(e));
		}
	}

}
