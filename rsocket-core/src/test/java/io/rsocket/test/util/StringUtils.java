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

package io.rsocket.test.util;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public final class StringUtils {

  private static final String CANDIDATE_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

  private StringUtils() {}

  public static String getRandomString(int size) {
    return ThreadLocalRandom.current()
        .ints(size, 0, CANDIDATE_CHARS.length())
        .mapToObj(index -> ((Character) CANDIDATE_CHARS.charAt(index)).toString())
        .collect(Collectors.joining());
  }
}
