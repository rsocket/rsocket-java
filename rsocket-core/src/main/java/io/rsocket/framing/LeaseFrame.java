/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.framing;

import static io.rsocket.framing.FrameType.LEASE;
import static io.rsocket.util.RecyclerFactory.createRecycler;
import static java.lang.Math.toIntExact;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.rsocket.util.NumberUtils;
import java.time.Duration;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An RSocket {@code LEASE} frame.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#lease-frame-0x02">Lease
 *     Frame</a>
 */
public final class LeaseFrame extends AbstractRecyclableMetadataFrame<LeaseFrame> {

  private static final int OFFSET_TIME_TO_LIVE = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final int OFFSET_NUMBER_OF_REQUESTS = OFFSET_TIME_TO_LIVE + Integer.BYTES;

  private static final int OFFSET_METADATA = OFFSET_NUMBER_OF_REQUESTS + Integer.BYTES;

  private static final Recycler<LeaseFrame> RECYCLER = createRecycler(LeaseFrame::new);

  private LeaseFrame(Handle<LeaseFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code LEASE} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code LEASE} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static LeaseFrame createLeaseFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code LEASE} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param timeToLive the validity of lease from time of reception
   * @param numberOfRequests the number of requests that may be sent until the next lease
   * @param metadata the metadata
   * @return the {@code LEASE} frame
   * @throws IllegalArgumentException if {@code timeToLive} is not a positive duration
   * @throws IllegalArgumentException if {@code numberOfRequests} is not positive
   * @throws NullPointerException if {@code byteBufAllocator} or {@code timeToLive} is {@code null}
   * @throws IllegalArgumentException if {@code timeToLive} is not a positive duration or {@code
   *     numberOfRequests} is not positive
   */
  public static LeaseFrame createLeaseFrame(
      ByteBufAllocator byteBufAllocator,
      Duration timeToLive,
      int numberOfRequests,
      @Nullable ByteBuf metadata) {

    Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    Objects.requireNonNull(timeToLive, "timeToLive must not be null");
    NumberUtils.requirePositive(timeToLive.toMillis(), "timeToLive must be a positive duration");
    NumberUtils.requirePositive(numberOfRequests, "numberOfRequests must be positive");

    ByteBuf byteBuf =
        createFrameTypeAndFlags(byteBufAllocator, LEASE)
            .writeInt(toIntExact(timeToLive.toMillis()))
            .writeInt(numberOfRequests);

    byteBuf = appendMetadata(byteBuf, metadata);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  /**
   * Returns the number of requests
   *
   * @return the number of requests
   */
  public int getNumberOfRequests() {
    return getByteBuf().getInt(OFFSET_NUMBER_OF_REQUESTS);
  }

  /**
   * Returns the time to live.
   *
   * @return the time to live
   */
  public Duration getTimeToLive() {
    return Duration.ofMillis(getByteBuf().getInt(OFFSET_TIME_TO_LIVE));
  }

  @Override
  public @Nullable ByteBuf getUnsafeMetadata() {
    return getMetadata(OFFSET_METADATA);
  }

  @Override
  public String toString() {
    return "LeaseFrame{"
        + "timeToLive="
        + getTimeToLive()
        + ", numberOfRequests="
        + getNumberOfRequests()
        + ", metadata="
        + mapMetadata(ByteBufUtil::hexDump)
        + +'}';
  }
}
