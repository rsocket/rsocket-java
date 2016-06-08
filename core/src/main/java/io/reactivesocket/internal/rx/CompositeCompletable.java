package io.reactivesocket.internal.rx;

import java.util.HashSet;
import java.util.Set;

import io.reactivesocket.rx.Completable;

/**
 * A Completable container that can hold onto multiple other Completables.
 */
public final class CompositeCompletable implements Completable {

	// protected by synchronized
	private boolean completed = false;
	private Throwable error = null;
	final Set<Completable> resources = new HashSet<>();

	public CompositeCompletable() {

	}

	public void add(Completable d) {
		boolean terminal = false;
		synchronized (this) {
			if (error != null || completed) {
				terminal = true;
			} else {
				resources.add(d);
			}
		}
		if (terminal) {
			if (error != null) {
				d.error(error);
			} else {
				d.success();
			}
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
	public void success() {
		Completable[] cs = null;
		synchronized (this) {
			if (error == null) {
				completed = true;
				cs = resources.toArray(new Completable[] {});
				resources.clear();
			}
		}
		if (cs != null) {
			for (Completable c : cs) {
				c.success();
			}
		}
	}

	@Override
	public void error(Throwable e) {
		Completable[] cs = null;
		synchronized (this) {
			if (error == null && !completed) {
				error = e;
				cs = resources.toArray(new Completable[] {});
				resources.clear();
			}
		}
		if (cs != null) {
			for (Completable c : cs) {
				c.error(e);
			}
		}
	}
}