/**
 * Copyright 2015 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observer;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

/**
 * Multicast eventbus that serializes incoming events.
 */
public class SerializedEventBus {

	private final CopyOnWriteArrayList<Observer<Frame>> os = new CopyOnWriteArrayList<>();
	private Subject<Frame, Frame> s;
	
	public SerializedEventBus() {
		s = PublishSubject.<Frame>create().toSerialized();
		s.subscribe(f-> {
			for (Observer<Frame> o : os) {
				o.onNext(f);
			}	
		});
	}
	
	public void send(Frame f) {
		s.onNext(f);
	}

	public void add(Observer<Frame> o) {
		os.add(o);
	}

	public void add(Consumer<Frame> f) {
		add(new Observer<Frame>() {

			@Override
			public void onNext(Frame t) {
				f.accept(t);
			}

			@Override
			public void onError(Throwable e) {

			}

			@Override
			public void onComplete() {

			}

			@Override
			public void onSubscribe(Disposable d) {
				// TODO Auto-generated method stub

			}

		});
	}

	public void remove(Observer<Frame> o) {
		os.remove(o);
	}
}