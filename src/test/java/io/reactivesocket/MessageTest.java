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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

public class MessageTest {

	@Test
	public void testWriteThenRead() {
		Message f = Message.from(1, MessageType.REQUEST_RESPONSE, "hello");
		assertEquals("hello", f.getMessage());
		assertEquals(MessageType.REQUEST_RESPONSE, f.getMessageType());
		assertEquals(1, f.getStreamId());

		ByteBuffer b = f.getBytes();

		Message f2 = Message.from(b);
		assertEquals("hello", f2.getMessage());
		assertEquals(MessageType.REQUEST_RESPONSE, f2.getMessageType());
		assertEquals(1, f2.getStreamId());
	}

	@Test
	public void testWrapMessage() {
		Message f = Message.from(1, MessageType.REQUEST_RESPONSE, "hello");

		f.wrap(2, MessageType.COMPLETE, "done");
		assertEquals("done", f.getMessage());
		assertEquals(MessageType.COMPLETE, f.getMessageType());
		assertEquals(2, f.getStreamId());
	}

	@Test
	public void testWrapBytes() {
		Message f = Message.from(1, MessageType.REQUEST_RESPONSE, "hello");
		Message f2 = Message.from(20, MessageType.COMPLETE, "another");

		ByteBuffer b = f2.getBytes();
		f.wrap(b);

		assertEquals("another", f.getMessage());
		assertEquals(MessageType.COMPLETE, f.getMessageType());
		assertEquals(20, f.getStreamId());
	}
}
