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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.internal.frame.PayloadFragmenter;
import io.reactivesocket.internal.rx.QueueDrainHelper;
import io.reactivesocket.internal.rx.SerializedSubscriber;
import io.reactivesocket.internal.rx.SubscriptionArbiter;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

public class FragmentedPublisher implements Publisher<Frame> {

	private volatile Subscriber<Frame> downstream;
	private volatile SubscriptionArbiter sa;
	private final AtomicInteger wip = new AtomicInteger();

	// TODO use better data structures
	private final List<InnerSubscriber> subscribers = Collections.synchronizedList(new ArrayList<InnerSubscriber>(16));
	private final Queue<InnerSubscriber> toRemove = new ConcurrentLinkedQueue<InnerSubscriber>();

	private int index = 0;
	private static final Object COMPLETED = new Object();

	@Override
	public void subscribe(Subscriber<? super Frame> child) {
		SerializedSubscriber<Frame> ssub = new SerializedSubscriber<>(child);
		sa = new SubscriptionArbiter();
		sa.setSubscription(new Subscription() {

			@Override
			public void request(long n) {
				tryEmit();
			}

			@Override
			public void cancel() {
				sa.cancel();
				for (InnerSubscriber is : subscribers) {
					is._s.cancel();
				}
			}

		});
		ssub.onSubscribe(sa);
		downstream = ssub;
	}

	public void tryEmit() {
		QueueDrainHelper.queueDrainLoop(wip,
				this::drainInnerSubscribers, /* drain if we get the emission lock */
				() -> {
					/* nothing to do if another thread has the emission lock */} ,
				this::drainInnerSubscribers /* drain if others tried while we were emitting */);
	}

	private void drainInnerSubscribers() {
		long r = sa.getRequested();
		int numItems = subscribers.size();
		int emitted = 0;
		int startIndex = index;
		if (subscribers.size() == 0) {
			return;
		}
		while (emitted < r) {
			if (index >= subscribers.size()) {
				index = 0;
			}
			InnerSubscriber is = subscribers.get(index++);
			numItems--;
			if (is == null) {
				break;
			}
			emitted += is.drain(r - emitted);
			if (numItems == 0) {
				break;
			}
			if (index == startIndex) {
				// looped around, break out so this thread isn't starved
				break;
			}
		}
		sa.produced(emitted);
		for (InnerSubscriber is : toRemove) {
			subscribers.remove(is);
			toRemove.remove(is);
		}
	}

	/**
	 * Horizontally Unbounded submission of Publisher. This means as many "concurrent" outputs as wanted.
	 * <p>
	 * This is ultimately controlled by lease semantics which controls how many streams (requests) are in-flight.
	 * 
	 * @param t
	 */
	public void submit(Publisher<Frame> t) {
		if (downstream == null) {
			throw new IllegalStateException("Downstream has not yet subscribed. Please await() subscription");
		}

		// horizontally no backpressure, we subscribe to all incoming Publishers, and then backpressure their individual streams
		InnerSubscriber is = new InnerSubscriber(this);
		subscribers.add(is);
		t.subscribe(is);
	}

	/**
	 * Asynchronously subscribe to Frames, buffer them internally if needed.
	 */
	private static final class InnerSubscriber implements Subscriber<Frame> {
		private static final long serialVersionUID = 1L;
		private final AtomicLong outstanding = new AtomicLong();
		// TODO replace this as this is very inefficient
		final OneToOneConcurrentArrayQueue<Object> q = new OneToOneConcurrentArrayQueue<Object>(128);
		final FragmentedPublisher parent;
		private Subscription _s;
		static final int BATCH = 128;

		public InnerSubscriber(FragmentedPublisher parent) {
			this.parent = parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			_s = s;
			// we manage our own rate since the transport should go as fast as it can, no application level flow control here
			s.request(BATCH);
			outstanding.set(BATCH);
		}

		@Override
		public void onNext(Frame frame) {
			FrameType type = frame.getType();
			int streamId = frame.getStreamId();
			if (PayloadFragmenter.requiresFragmenting(Frame.METADATA_MTU, Frame.DATA_MTU, frame)) {
				nextFragmented(frame, type, streamId);
			} else {
				nextUnfragmented(frame);
			}
		}

		private void nextUnfragmented(Frame frame) {
			QueueDrainHelper.queueDrainLoop(parent.wip,
					/* fast-path if no contention when trying to emit */
					() -> {
						if (parent.sa.getRequested() == 0) {
							q.add(frame);
							return;
						}
						if (q.size() == 0) {
							parent.downstream.onNext(frame);
							parent.sa.produced(1);
							outstanding.decrementAndGet();
							InnerSubscriber.this.requestMoreIfNeeded();
						} else {
							// enqueue, then drain if there are already things in the queue
							q.add(frame);
							drainRequestedAndRequestMoreIfNeeded();
						}
					} ,
					/* if contended, then just enqueue */
					() -> {
						q.add(frame);
					} ,
					/* if another thread enqueued while emitting above, then this will have a chance to drain after */
					() -> {
						drainRequestedAndRequestMoreIfNeeded();
					});
		}

		private void nextFragmented(Frame frame, FrameType type, int streamId) {
			// not reusing each time since I need the Iterator state stored through request(n) and can have several in a queue
			PayloadFragmenter fragmenter = new PayloadFragmenter(Frame.METADATA_MTU, Frame.DATA_MTU);
			if (FrameType.NEXT_COMPLETE.equals(type)) {
				fragmenter.resetForResponseComplete(streamId, frame);
			} else {
				fragmenter.resetForResponse(streamId, frame);
			}

			QueueDrainHelper.queueDrainLoop(parent.wip,
					/* fast-path if no contention when trying to emit */
					() -> {
						if (parent.sa.getRequested() == 0) {
							q.add(frame);
							return;
						}
						if (q.size() == 0) {
							long r = parent.sa.getRequested();
							long emitted = 0;
							// emit as much of iterable as requested allows
							for (int i = 0; i < r; i++) { // TODO limit so we don't head-of-line block
								if (fragmenter.hasNext()) {
									parent.downstream.onNext(fragmenter.next());
									emitted++;
								} else {
									break;
								}
								parent.sa.produced(emitted);
								outstanding.addAndGet(-emitted);
								InnerSubscriber.this.requestMoreIfNeeded();
							}
							if (fragmenter.hasNext()) {
								// not finished so enqueue
								q.add(fragmenter);
							}
						} else {
							// enqueue, then drain if there are already things in the queue
							q.add(fragmenter);
							drainRequestedAndRequestMoreIfNeeded();
						}
					} ,
					/* if contended, then just enqueue */
					() -> {
						q.add(fragmenter);
					} ,
					/* if another thread enqueued while emitting above, then this will have a chance to drain after */
					() -> {
						drainRequestedAndRequestMoreIfNeeded();
					});
		}

		private void drainRequestedAndRequestMoreIfNeeded() {
			long emitted = drain(parent.sa.getRequested());
			parent.sa.produced(emitted);
			outstanding.addAndGet(-emitted);
			InnerSubscriber.this.requestMoreIfNeeded();
		}

		public long drain(long maxToDrain) {
			long emitted = 0;
			while (emitted < maxToDrain) {
				Object o = q.peek();
				if (o == null) {
					break;
				}
				if (o instanceof Frame) {
					parent.downstream.onNext((Frame) o);
					emitted++;
					q.poll(); // pop it off the queue
				} else if (o == COMPLETED) {
					parent.toRemove.add(InnerSubscriber.this);
					break;
				} else {
					@SuppressWarnings("unchecked")
					Iterator<Frame> ifs = (Iterator<Frame>) o;
					while (ifs.hasNext()) {
						parent.downstream.onNext(ifs.next());
						emitted++;
						if (emitted == maxToDrain) {
							break;
						}
					}
					if (!ifs.hasNext()) {
						// finished, so remove it
						q.poll();
					}
				}
			}
			return emitted;
		}

		private void requestMoreIfNeeded() {
			long current = outstanding.get();
			if (current < 20) {
				long d = BATCH - current;
				_s.request(d);
				outstanding.addAndGet(d);
			}
		}

		@Override
		public void onError(Throwable t) {
			parent.sa.cancel();
			parent.downstream.onError(t);
			parent.subscribers.remove(this);
		}

		@Override
		public void onComplete() {
			if (q.size() == 0) {
				parent.subscribers.remove(this);
			} else {
				q.add(COMPLETED);
			}
		}
	}

}
