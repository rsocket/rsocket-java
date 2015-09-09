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
import io.reactivesocket.internal.UnicastSubject;
import io.reactivex.subscribers.TestSubscriber;

public class UnicastSubjectTest {

	@Test
	public void testSubscribeReceiveValue() {
		Frame f = TestUtil.utf8EncodedResponseFrame(1, FrameType.NEXT_COMPLETE, "response");
		UnicastSubject us = UnicastSubject.create();
		TestSubscriber<Frame> ts = new TestSubscriber<>();
		us.subscribe(ts);
		us.onNext(f);
		ts.assertValue(f);
		ts.assertNotTerminated();
	}

	@Test(expected = NullPointerException.class)
	public void testNullPointerSendingWithoutSubscriber() {
		Frame f = TestUtil.utf8EncodedResponseFrame(1, FrameType.NEXT_COMPLETE, "response");
		UnicastSubject us = UnicastSubject.create();
		us.onNext(f);
	}

	@Test
	public void testIllegalStateIfMultiSubscribe() {
		UnicastSubject us = UnicastSubject.create();
		TestSubscriber<Frame> f1 = new TestSubscriber<>();
		us.subscribe(f1);
		TestSubscriber<Frame> f2 = new TestSubscriber<>();
		us.subscribe(f2);

		f1.assertNotTerminated();
		f2.assertError(IllegalStateException.class);
	}

}
