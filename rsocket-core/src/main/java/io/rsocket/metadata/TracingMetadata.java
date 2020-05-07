/*
 * Copyright 2015-2020 the original author or authors.
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

package io.rsocket.metadata;

public final class TracingMetadata {

  final long traceIdHigh;
  final long traceId;
  private final boolean hasParentId;
  final long parentId;
  final long spanId;
  final boolean isEmpty;
  final boolean isNotSampled;
  final boolean isSampled;
  final boolean isDebug;

  TracingMetadata(
      long traceIdHigh,
      long traceId,
      long spanId,
      boolean hasParentId,
      long parentId,
      boolean isEmpty,
      boolean isNotSampled,
      boolean isSampled,
      boolean isDebug) {
    this.traceIdHigh = traceIdHigh;
    this.traceId = traceId;
    this.spanId = spanId;
    this.hasParentId = hasParentId;
    this.parentId = parentId;
    this.isEmpty = isEmpty;
    this.isNotSampled = isNotSampled;
    this.isSampled = isSampled;
    this.isDebug = isDebug;
  }

  /** When non-zero, the trace containing this span uses 128-bit trace identifiers. */
  public long traceIdHigh() {
    return traceIdHigh;
  }

  /** Unique 8-byte identifier for a trace, set on all spans within it. */
  public long traceId() {
    return traceId;
  }

  /** Indicates if the parent's {@link #spanId} or if this the root span in a trace. */
  public final boolean hasParent() {
    return hasParentId;
  }

  /** Returns the parent's {@link #spanId} where zero implies absent. */
  public long parentId() {
    return parentId;
  }

  /**
   * Unique 8-byte identifier of this span within a trace.
   *
   * <p>A span is uniquely identified in storage by ({@linkplain #traceId}, {@linkplain #spanId}).
   */
  public long spanId() {
    return spanId;
  }

  /** Indicates that trace IDs should be accepted for tracing. */
  public boolean isSampled() {
    return isSampled;
  }

  /** Indicates that trace IDs should be force traced. */
  public boolean isDebug() {
    return isDebug;
  }

  /** Includes that there is sampling information and no trace IDs. */
  public boolean isEmpty() {
    return isEmpty;
  }

  /**
   * Indicated that sampling decision is present. If {@code false} means that decision is unknown
   * and says explicitly that {@link #isDebug()} and {@link #isSampled()} also returns {@code
   * false}.
   */
  public boolean isDecided() {
    return isNotSampled || isDebug || isSampled;
  }
}
