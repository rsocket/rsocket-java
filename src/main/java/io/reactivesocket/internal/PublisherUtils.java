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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.internal.rx.BackpressureHelper;
import io.reactivesocket.internal.rx.BackpressureUtils;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.internal.rx.SubscriptionHelper;

public class PublisherUtils {

	// TODO: be better about using scheduler for this
	public static final ScheduledExecutorService SCHEDULER_THREAD = Executors.newScheduledThreadPool(1,
		(r) -> {
			final Thread thread = new Thread(r);

			thread.setDaemon(true);

			return thread;
		});

	public static final Publisher<Frame> errorFrame(int streamId, Throwable e) {
		return (Subscriber<? super Frame> s) -> {
			s.onSubscribe(new Subscription() {

				@Override
				public void request(long n) {
					if (n > 0) {
						s.onNext(Frame.Error.from(streamId, e));
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

	private final static ByteBuffer EMPTY_BYTES = ByteBuffer.allocate(0);

	public static final Publisher<Payload> errorPayload(Throwable e) {
		return (Subscriber<? super Payload> s) -> {
			s.onSubscribe(new Subscription() {

				@Override
				public void request(long n) {
					if (n > 0) {
						Payload errorPayload = new Payload() {

							@Override
							public ByteBuffer getData() {
								final byte[] bytes = e.getMessage().getBytes();
								final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
								return byteBuffer;
							}

							@Override
							public ByteBuffer getMetadata() {
								return EMPTY_BYTES;
							}

						};
						s.onNext(errorPayload);
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

	public static final Publisher<Void> errorVoid(Throwable e) {
		return (Subscriber<? super Void> s) -> {
			s.onSubscribe(new Subscription() {

				@Override
				public void request(long n) {
				}

				@Override
				public void cancel() {
					// ignoring as nothing to do
				}

			});
			s.onError(e);

		};
	}

	public static final Publisher<Frame> just(Frame frame) {
		return (Subscriber<? super Frame> s) -> {
			s.onSubscribe(new Subscription() {

				boolean completed = false;

				@Override
				public void request(long n) {
					if (!completed && n > 0) {
						completed = true;
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

	public static final Publisher<Frame> keepaliveTicker(final int interval, final TimeUnit timeUnit) {
		return (Subscriber<? super Frame> s) -> {
			s.onSubscribe(new Subscription()
			{
				final AtomicLong requested = new AtomicLong(0);
				final AtomicBoolean started = new AtomicBoolean(false);
				volatile ScheduledFuture ticker;

				public void request(long n)
				{
					BackpressureUtils.getAndAddRequest(requested, n);
					if (started.compareAndSet(false, true))
					{
						ticker = SCHEDULER_THREAD.scheduleWithFixedDelay(() -> {
							final long value = requested.getAndDecrement();

							if (0 < value)
							{
								s.onNext(Frame.Keepalive.from(Frame.NULL_BYTEBUFFER, true));
							}
							else
							{
								requested.getAndIncrement();
							}
						}, interval, interval, timeUnit);
					}
				}

				public void cancel()
				{
					// only used internally and so should not be called before request is done. Race condition exists!
					if (null != ticker)
					{
						ticker.cancel(true);
					}
				}
			});
		};
	}
	
	public static final Publisher<Frame> fromIterable(Iterable<Frame> is) {
		return new PublisherIterableSource<>(is);
	}
	
	public static final class PublisherIterableSource<T> extends AtomicBoolean implements Publisher<T> {
	    /** */
	    private static final long serialVersionUID = 9051303031779816842L;
	    
	    final Iterable<? extends T> source;
	    public PublisherIterableSource(Iterable<? extends T> source) {
	        this.source = source;
	    }
	    
	    @Override
	    public void subscribe(Subscriber<? super T> s) {
	        Iterator<? extends T> it;
	        try {
	            it = source.iterator();
	        } catch (Throwable e) {
	            EmptySubscription.error(e, s);
	            return;
	        }
	        boolean hasNext;
	        try {
	            hasNext = it.hasNext();
	        } catch (Throwable e) {
	            EmptySubscription.error(e, s);
	            return;
	        }
	        if (!hasNext) {
	            EmptySubscription.complete(s);
	            return;
	        }
	        s.onSubscribe(new IteratorSourceSubscription<>(it, s));
	    }
	    
	    static final class IteratorSourceSubscription<T> extends AtomicLong implements Subscription {
	        /** */
	        private static final long serialVersionUID = 8931425802102883003L;
	        final Iterator<? extends T> it;
	        final Subscriber<? super T> subscriber;
	        
	        volatile boolean cancelled;
	        
	        public IteratorSourceSubscription(Iterator<? extends T> it, Subscriber<? super T> subscriber) {
	            this.it = it;
	            this.subscriber = subscriber;
	        }
	        @Override
	        public void request(long n) {
	            if (SubscriptionHelper.validateRequest(n)) {
	                return;
	            }
	            if (BackpressureHelper.add(this, n) != 0L) {
	                return;
	            }
	            long r = n;
	            long r0 = n;
	            final Subscriber<? super T> subscriber = this.subscriber;
	            final Iterator<? extends T> it = this.it;
	            for (;;) {
	                if (cancelled) {
	                    return;
	                }

	                long e = 0L;
	                while (r != 0L) {
	                    T v;
	                    try {
	                        v = it.next();
	                    } catch (Throwable ex) {
	                        subscriber.onError(ex);
	                        return;
	                    }
	                    
	                    if (v == null) {
	                        subscriber.onError(new NullPointerException("Iterator returned a null element"));
	                        return;
	                    }
	                    
	                    subscriber.onNext(v);
	                    
	                    if (cancelled) {
	                        return;
	                    }
	                    
	                    boolean hasNext;
	                    try {
	                        hasNext = it.hasNext();
	                    } catch (Throwable ex) {
	                        subscriber.onError(ex);
	                        return;
	                    }
	                    if (!hasNext) {
	                        subscriber.onComplete();
	                        return;
	                    }
	                    
	                    r--;
	                    e--;
	                }
	                if (e != 0L && r0 != Long.MAX_VALUE) {
	                    r = addAndGet(e);
	                }
	                if (r == 0L) {
	                    break;
	                }
	            }
	        }
	        @Override
	        public void cancel() {
	            cancelled = true;
	        }
	    }
	}


}
