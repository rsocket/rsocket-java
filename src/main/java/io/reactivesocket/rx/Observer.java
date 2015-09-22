package io.reactivesocket.rx;

public interface Observer<T> {

	public void onNext(T t);
	
	public void onError(Throwable e);
	
	public void onComplete();
	
	public void onSubscribe(Disposable d);
}
