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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.Frame;

public class PublisherUtils {

	public static final Publisher<Frame> error(Frame requestFrame, Throwable e) {
		return (Subscriber<? super Frame> s) -> {
			s.onSubscribe(new Subscription() {

				@Override
				public void request(long n) {
					if (n > 0) {
						s.onNext(Frame.from(requestFrame.getStreamId(), e));
						s.onComplete();
					}
				}

				@Override
				public void cancel() {
					// ignoring as nothing to do
				}

			});

		};
	}

	public static final Publisher<Frame> just(Frame frame) {
		return (Subscriber<? super Frame> s) -> {
			s.onSubscribe(new Subscription() {

				@Override
				public void request(long n) {
					if (n > 0) {
						s.onNext(frame);
						s.onComplete();
					}
				}

				@Override
				public void cancel() {
					// ignoring as nothing to do
				}

			});

		};
	}

	public static final <T> Publisher<T> empty() {
		return (Subscriber<? super T> s) -> {
			s.onSubscribe(new Subscription() {

				@Override
				public void request(long n) {
				}

				@Override
				public void cancel() {
					// ignoring as nothing to do
				}

			});
			s.onComplete(); // TODO confirm this is okay with ReactiveStream spec to send immediately after onSubscribe (I think so since no data is being sent so requestN doesn't matter)
		};

	}

}
