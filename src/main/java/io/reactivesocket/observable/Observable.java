package io.reactivesocket.observable;

public interface Observable<T> {

	public void subscribe(Observer<T> o);
}
