package io.reactivesocket.rx;

public interface Completable {

	public abstract void success();
	
	public abstract void error(Throwable e);
	
}
