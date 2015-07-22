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

enum MessageType {
	// DO NOT REORDER OR INSERT NEW ELEMENTS. THE ORDINAL IS PART OF THE PROTOCOL
	SUBSCRIBE_REQUEST_RESPONSE, SUBSCRIBE_STREAM, STREAM_REQUEST, DISPOSE, NEXT_COMPLETE, NEXT, ERROR, COMPLETE;
	public static MessageType[] values = MessageType.values(); // cached for performance
}