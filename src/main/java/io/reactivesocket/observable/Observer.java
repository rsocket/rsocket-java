package io.reactivesocket.observable;

public interface Observer<T> {

	public void onNext(T t);
	
	public void onError(Throwable e);
	
	public void onComplete();
	
	public void onSubscribe(Disposable d);
}
