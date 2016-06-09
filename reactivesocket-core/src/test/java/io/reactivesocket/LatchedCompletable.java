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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.reactivesocket.rx.Completable;

public class LatchedCompletable implements Completable {

    	final CountDownLatch latch;
    	
    	public LatchedCompletable(int count) {
    		this.latch = new CountDownLatch(count);
    	}
    	
		@Override
		public void success() {
			latch.countDown();
		}

		@Override
		public void error(Throwable e) {
			System.err.println("Error waiting for Requester");
			e.printStackTrace();
			latch.countDown();				
		}

		public void await() throws InterruptedException {
			latch.await();
		}

		public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
			return latch.await(timeout, unit);
		}
		
		
}
