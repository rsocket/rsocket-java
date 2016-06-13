package io.reactivesocket.rx;

import reactor.core.flow.Cancellation;

public interface Observer<T> {

	public void onNext(T t);
	
	public void onError(Throwable e);
	
	public void onComplete();
	
	public void onSubscribe(Cancellation d);
}
