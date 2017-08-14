/*
 * Copyright 2016 Facebook, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.rsocket.tckdrivers.common;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Simple implementation of a tuple
 *
 * @param <K> key
 * @param <V> value
 */
public class Tuple<K, V> {

  private final K k;
  private final V v;

  public Tuple(K k, V v) {
    this.k = k;
    this.v = v;
  }

  public K getK() {
    return this.k;
  }

  public V getV() {
    return this.v;
  }

  @Override
  public boolean equals(Object o) {
    if (!o.getClass().isInstance(this)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    Tuple<K, V> temp = (Tuple<K, V>) o;
    return temp.getV().equals(this.getV()) && temp.getK().equals(this.getK());
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(this.getK().hashCode())
        .append(this.getV().hashCode())
        .toHashCode();
  }

  @Override
  public String toString() {
    return getV().toString() + "," + getK().toString();
  }
}
