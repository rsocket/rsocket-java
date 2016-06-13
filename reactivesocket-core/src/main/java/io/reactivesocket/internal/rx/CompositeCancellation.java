package io.reactivesocket.internal.rx;

import java.util.HashSet;
import java.util.Set;

import io.reactivesocket.rx.Completable;
import reactor.core.flow.Cancellation;

/**
 * A Cancellation container that can hold onto multiple other Cancellations.
 */
public final class CompositeCancellation implements Cancellation {

	// protected by synchronized
	private boolean disposed = false;
	final Set<Cancellation> resources = new HashSet<>();

	public CompositeCancellation() {

	}

	public void add(Cancellation d) {
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
		Cancellation[] cs;
		synchronized (this) {
			disposed = true;
			cs = resources.toArray(new Cancellation[] {});
			resources.clear();
		}
		for (Cancellation d : cs) {
			d.dispose();
		}
	}

}