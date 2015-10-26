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
package io.reactivesocket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.rx.Completable;

/**
 * Connection that by defaults only calls request(1) on a Publisher to addOutput. Any further must be done via requestMore(n)
 * <p>
 * NOTE: This should ONLY be used for 1 test at a time as it maintains state. Call close() when done.
 */
public class TestConnectionWithControlledRequestN extends TestConnection {

	public List<Subscription> subscriptions = Collections.synchronizedList(new ArrayList<Subscription>());
	public AtomicLong emitted = new AtomicLong();
	public AtomicLong requested = new AtomicLong();

	@Override
	public void addOutput(Publisher<Frame> o, Completable callback) {
		System.out.println("TestConnectionWithControlledRequestN => addOutput");
		o.subscribe(new Subscriber<Frame>() {

			volatile Subscription _s = null;
			public AtomicLong sEmitted = new AtomicLong();

			@Override
			public void onSubscribe(Subscription s) {
				_s = new Subscription() {

					@Override
					public void request(long n) {
						requested.addAndGet(n);
						s.request(n);
					}

					@Override
					public void cancel() {
						subscriptions.remove(_s);
						s.cancel();
					}

				};
				subscriptions.add(_s);
				_s.request(1);
			}

			@Override
			public void onNext(Frame t) {
				emitted.incrementAndGet();
				sEmitted.incrementAndGet();
				write.send(t);
			}

			@Override
			public void onError(Throwable t) {
				subscriptions.remove(_s);
				callback.error(t);
			}

			@Override
			public void onComplete() {
				System.out.println("TestConnectionWithControlledRequestN => complete, emitted: " + sEmitted.get());
				subscriptions.remove(_s);
				callback.success();
			}

		});
	}

	@Override
	public void addOutput(Frame f, Completable callback) {
		emitted.incrementAndGet();
		write.send(f);
		callback.success();
	}

	public boolean awaitSubscription(int timeInMillis) {
		long start = System.currentTimeMillis();
		while (subscriptions.size() == 0) {
			Thread.yield();
			if(System.currentTimeMillis() - start > timeInMillis) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Request more against the first subscription. This will ONLY request against the oldest Subscription, one at a time.
	 * <p>
	 * When one completes, it does NOT propagate request(n) to the next. Thus, this assumes unit tests where you know what you are doing with request(n).
	 * 
	 * @param n
	 */
	public void requestMore(int n) {
		if (subscriptions.size() == 0) {
			throw new IllegalStateException("no subscriptions to request from");
		}
		subscriptions.get(0).request(n);
	}

}