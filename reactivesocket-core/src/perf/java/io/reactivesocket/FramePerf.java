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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class FramePerf {

	public static Frame utf8EncodedFrame(final int streamId, final FrameType type, final String data)
	{
		final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
		final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		final Payload payload = new Payload()
		{
			public ByteBuffer getData()
			{
				return byteBuffer;
			}

			public ByteBuffer getMetadata()
			{
				return Frame.NULL_BYTEBUFFER;
			}
		};

		return Frame.Response.from(streamId, type, payload);
	}

	/**
	 * Test encoding of "hello" frames/second with a new string->byte encoding each time
	 * 
	 * @param input
	 * @return
	 * @throws InterruptedException
	 */
	@Benchmark
	public Frame encodeNextCompleteHello(Input input) throws InterruptedException {
		return utf8EncodedFrame(0, FrameType.NEXT_COMPLETE, "hello");
	}
	
	/**
	 * Test encoding of Frame without any overhead with byte[] or ByteBuffer by reusing the same ByteBuffer
	 * 
	 * @param input
	 * @return
	 */
	@Benchmark
	public Frame encodeStaticHelloIntoFrame(Input input) {
		input.HELLO.position(0);
		return Frame.Response.from(0, FrameType.NEXT_COMPLETE, input.HELLOpayload);
	}
	
	@State(Scope.Thread)
	public static class Input {
		/**
		 * Use to consume values when the test needs to return more than a single value.
		 */
		public Blackhole bh;
		
		public ByteBuffer HELLO = ByteBuffer.wrap("HELLO".getBytes(StandardCharsets.UTF_8));
		public Payload HELLOpayload = new Payload()
		{
			public ByteBuffer getData()
			{
				return HELLO;
			}

			public ByteBuffer getMetadata()
			{
				return Frame.NULL_BYTEBUFFER;
			}
		};

		@Setup
		public void setup(Blackhole bh) {
			this.bh = bh;
		}
	}

}
