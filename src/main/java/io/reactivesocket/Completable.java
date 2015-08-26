package io.reactivesocket;

public interface Completable {

	public abstract void success();
	
	public abstract void error(Throwable e);
	
}
