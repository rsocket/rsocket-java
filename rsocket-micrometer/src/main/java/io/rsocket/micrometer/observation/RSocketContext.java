/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.micrometer.observation;

import io.micrometer.api.instrument.observation.Observation;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.frame.FrameType;
import reactor.util.context.ContextView;

class RSocketContext extends Observation.Context {

	final Payload payload;

	final ByteBuf metadata;

	final FrameType frameType;

	final Side side;

	Payload modifiedPayload;

	RSocketContext(Payload payload, ByteBuf metadata, FrameType frameType, Side side) {
		this.payload = payload;
		this.metadata = metadata;
		this.frameType = frameType;
		this.side = side;
	}

	enum Side {
		REQUESTER, RESPONDER
	}
}
