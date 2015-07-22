package io.reactivesocket;

import static rx.RxReactiveStreams.*;

import org.reactivestreams.Publisher;

import rx.Observable;
import rx.subjects.PublishSubject;

class TestConnection implements DuplexConnection {

	final PublishSubject<Message> toServer = PublishSubject.create();
	final PublishSubject<Message> toClient = PublishSubject.create();

	@Override
	public Publisher<Void> write(Message o) {
		toClient.onNext(o);
		return toPublisher(Observable.<Void> empty());
	}

	@Override
	public Publisher<Message> getInput() {
		return toPublisher(toServer);
	}
}