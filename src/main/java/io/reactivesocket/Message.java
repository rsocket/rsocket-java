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

import java.nio.ByteBuffer;

public class Message {

	private Message() {
	}

	// not final so we can reuse this object
	private ByteBuffer b;
	private int messageId;
	private MessageType type;
	private String message;

	public ByteBuffer getBytes() {
		return b;
	}

	public String getMessage() {
		if (type == null) {
			decode();
		}
		return message;
	}

	public int getMessageId() {
		if (type == null) {
			decode();
		}
		return messageId;
	}

	public MessageType getMessageType() {
		if (type == null) {
			decode();
		}
		return type;
	}

	/**
	 * Mutates this Frame to contain the given ByteBuffer
	 * 
	 * @param b
	 */
	public void wrap(ByteBuffer b) {
		this.messageId = -1;
		this.type = null;
		this.message = null;
		this.b = b;
	}

	/**
	 * Construct a new Frame from the given ByteBuffer
	 * 
	 * @param b
	 * @return
	 */
	public static Message from(ByteBuffer b) {
		Message f = new Message();
		f.b = b;
		return f;
	}

	/**
	 * Mutates this Frame to contain the given message.
	 * 
	 * @param messageId
	 * @param type
	 * @param message
	 */
	public void wrap(int messageId, MessageType type, String message) {
		this.messageId = messageId;
		this.type = type;
		this.message = message;
		this.b = getBytes(messageId, type, message);
	}

	/**
	 * Construct a new Frame with the given message.
	 * 
	 * @param messageId
	 * @param type
	 * @param message
	 * @return
	 */
	public static Message from(int messageId, MessageType type, String message) {
		Message f = new Message();
		f.b = getBytes(messageId, type, message);
		f.messageId = messageId;
		f.type = type;
		f.message = message;
		return f;
	}

	private static ByteBuffer getBytes(int messageId, MessageType type, String message) {
		// TODO replace with binary
		/**
		 * This is NOT how we want it for real. Just representing the idea for discussion.
		 */
		String s = "[" + type.ordinal() + "]" + getIdString(messageId) + message;
		// TODO stop allocating ... use flywheels
		return ByteBuffer.wrap(s.getBytes());
	}

	private static String getIdString(int id) {
		return "[" + id + "]|";
	}

	private void decode() {
		// TODO replace with binary
		/**
		 * This is NOT how we want it for real. Just representing the idea for discussion.
		 */
		String data = new String(b.array());
		int separator = data.indexOf('|');
		String prefix = data.substring(0, separator);
		this.type = MessageType.values[Integer.parseInt(prefix.substring(1, data.indexOf(']')))];
		this.messageId = Integer.parseInt(prefix.substring(prefix.lastIndexOf("[") + 1, prefix.length() - 1));
		this.message = data.substring(separator + 1, data.length());
	}

}
