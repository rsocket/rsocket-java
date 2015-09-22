package io.reactivesocket.internal;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.TestUtil;
import io.reactivesocket.internal.frame.PayloadBuilder;
import io.reactivex.Observable;
import io.reactivex.subscribers.TestSubscriber;

public class FragmentedPublisherTest {

	static String LARGE_STRING = "";

	static {
		for (int i = 0; i < 1000; i++) {
			LARGE_STRING += "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
		}
	}

	Frame HELLO_FRAME = TestUtil.utf8EncodedResponseFrame(1, FrameType.REQUEST_RESPONSE, "hello");
	Frame LARGE_FRAME = TestUtil.utf8EncodedResponseFrame(1, FrameType.REQUEST_RESPONSE, LARGE_STRING);

	Iterable<Frame> HELLO_ITERABLE_OF_3 = Arrays.asList(HELLO_FRAME, HELLO_FRAME, HELLO_FRAME);

	@Before
	public void setup() {
		System.out.println("-----------------------------------------------------------");
	}

	@Test
	public void testOneSingleRequestedBefore() {
		FragmentedPublisher fp = new FragmentedPublisher();

		TestSubscriber<Frame> ts = new TestSubscriber<Frame>(0L);
		fp.subscribe(ts);

		ts.request(1);

		fp.submit(PublisherUtils.just(HELLO_FRAME));

		ts.assertValue(HELLO_FRAME);
	}

	@Test
	public void testOneSingleRequestedAfter() {
		FragmentedPublisher fp = new FragmentedPublisher();

		TestSubscriber<Frame> ts = new TestSubscriber<Frame>(0L);
		fp.subscribe(ts);

		fp.submit(PublisherUtils.just(HELLO_FRAME));

		ts.assertNoValues();

		ts.request(1);

		ts.assertValue(HELLO_FRAME);
	}

	@Test
	public void testMultipleSingle() {
		FragmentedPublisher fp = new FragmentedPublisher();

		TestSubscriber<Frame> ts = new TestSubscriber<Frame>(0L);
		fp.subscribe(ts);

		ts.request(1);

		ts.assertNoValues();

		fp.submit(PublisherUtils.just(HELLO_FRAME));

		ts.assertValues(HELLO_FRAME);

		fp.submit(PublisherUtils.just(HELLO_FRAME));
		fp.submit(PublisherUtils.just(HELLO_FRAME));
		fp.submit(PublisherUtils.just(HELLO_FRAME));

		ts.assertValues(HELLO_FRAME);

		ts.request(4);

		ts.assertValues(HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME);

		fp.submit(PublisherUtils.just(HELLO_FRAME));
		fp.submit(PublisherUtils.just(HELLO_FRAME));

		ts.assertValues(HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME);

		ts.request(10);

		ts.assertValues(HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME);
	}

	@Test
	public void testOneStreamRequestedBefore() {
		FragmentedPublisher fp = new FragmentedPublisher();

		TestSubscriber<Frame> ts = new TestSubscriber<Frame>(0L);
		fp.subscribe(ts);

		ts.request(10);

		fp.submit(PublisherUtils.fromIterable(HELLO_ITERABLE_OF_3));

		ts.assertValues(HELLO_FRAME, HELLO_FRAME, HELLO_FRAME);
	}

	@Test
	public void testOneStreamRequestedAfter() {
		FragmentedPublisher fp = new FragmentedPublisher();

		TestSubscriber<Frame> ts = new TestSubscriber<Frame>(0L);
		fp.subscribe(ts);

		fp.submit(PublisherUtils.fromIterable(HELLO_ITERABLE_OF_3));

		ts.assertNoValues();

		ts.request(10);

		ts.assertValues(HELLO_FRAME, HELLO_FRAME, HELLO_FRAME);
	}

	@Test
	public void testMixture() {
		FragmentedPublisher fp = new FragmentedPublisher();

		TestSubscriber<Frame> ts = new TestSubscriber<Frame>(0L);
		fp.subscribe(ts);

		ts.request(1);

		ts.assertNoValues();

		fp.submit(PublisherUtils.just(HELLO_FRAME));

		ts.assertValues(HELLO_FRAME);

		fp.submit(PublisherUtils.just(HELLO_FRAME));
		fp.submit(PublisherUtils.fromIterable(HELLO_ITERABLE_OF_3));
		fp.submit(PublisherUtils.just(HELLO_FRAME));

		ts.assertValues(HELLO_FRAME);

		ts.request(4);

		ts.assertValues(HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME);

		fp.submit(PublisherUtils.fromIterable(HELLO_ITERABLE_OF_3));
		fp.submit(PublisherUtils.just(HELLO_FRAME));

		ts.assertValues(HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME);

		ts.request(10);

		ts.assertValues(HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME, HELLO_FRAME);
	}

	@Test
	public void testOneSingleFragmented() {
		FragmentedPublisher fp = new FragmentedPublisher();

		PayloadBuilder builder = new PayloadBuilder();

		TestSubscriber<Frame> ts = new TestSubscriber<Frame>(new Subscriber<Frame>() {

			@Override
			public void onSubscribe(Subscription s) {
			}

			@Override
			public void onNext(Frame t) {
				builder.append(t);
			}

			@Override
			public void onError(Throwable t) {
			}

			@Override
			public void onComplete() {
			}

		}, 0L);
		fp.subscribe(ts);

		ts.request(1);

		fp.submit(PublisherUtils.just(LARGE_FRAME));

		ts.assertValueCount(1);

		ts.request(100);

		ts.assertValueCount(4);

		Payload payload = builder.payload();
		assertEquals(TestUtil.byteToString(LARGE_FRAME.getData()), TestUtil.byteToString(payload.getData()));
	}
	
	@Test
	public void testLargeStream() {
		FragmentedPublisher fp = new FragmentedPublisher();

		TestSubscriber<Frame> ts = new TestSubscriber<Frame>(0L);
		fp.subscribe(ts);

		ts.request(Long.MAX_VALUE);

		fp.submit(Observable.range(0, 2000).map(i -> TestUtil.utf8EncodedResponseFrame(1, FrameType.NEXT, "")));

		ts.assertValueCount(2000);
	}
	
	@Test
	public void testLargeStreamCancel() {
		FragmentedPublisher fp = new FragmentedPublisher();

		TestSubscriber<Frame> ts = new TestSubscriber<Frame>(0L);
		Observable.fromPublisher(fp).take(200).subscribe(ts);

		ts.request(Long.MAX_VALUE);
		
		AtomicInteger count = new AtomicInteger();
		
		fp.submit(Observable.range(0, 2000)
				.doOnNext(i -> count.incrementAndGet())
				.map(i -> TestUtil.utf8EncodedResponseFrame(1, FrameType.NEXT, "")));

		
		assertEquals(200, count.get());
		ts.assertValueCount(200);
	}
}
