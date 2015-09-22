package io.reactivesocket.internal.rx;

import java.util.HashSet;
import java.util.Set;

import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;

/**
 * A Disposable container that can hold onto multiple other Disposables.
 */
public final class CompositeDisposable implements Disposable {

	// protected by synchronized
	private boolean disposed = false;
	final Set<Disposable> resources = new HashSet<>();

	public CompositeDisposable() {

	}

	public void add(Disposable d) {
		boolean isDisposed = false;
		synchronized (this) {
			if (disposed) {
				isDisposed = true;
			} else {
				resources.add(d);
			}
		}
		if (isDisposed) {
			d.dispose();
		}
	}

	public void remove(Completable d) {
		synchronized (this) {
			resources.remove(d);
		}
	}

	public void clear() {
		synchronized (this) {
			resources.clear();
		}
	}

	@Override
	public void dispose() {
		Disposable[] cs = null;
		synchronized (this) {
			disposed = true;
			cs = resources.toArray(new Disposable[] {});
			resources.clear();
		}
		for (Disposable d : cs) {
			d.dispose();
		}
	}

}