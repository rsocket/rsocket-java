package io.reactivesocket.rx;

public interface Observable<T> {

	public void subscribe(Observer<T> o);
}
