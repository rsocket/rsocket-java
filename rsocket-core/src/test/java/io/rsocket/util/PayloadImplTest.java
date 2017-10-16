/*
 * Copyright 2016 Netflix, Inc.
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

package io.rsocket.util;

import static io.rsocket.util.PayloadImpl.textPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.rsocket.Payload;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

public class PayloadImplTest {
  private static final String DATA_VAL = "data";
  private static final String METADATA_VAL = "metadata";

  @Test
  public void testReuse() {
    PayloadImpl p = new PayloadImpl(DATA_VAL, METADATA_VAL);
    assertDataAndMetadata(p, DATA_VAL, METADATA_VAL);
    assertDataAndMetadata(p, DATA_VAL, METADATA_VAL);
  }

  @Test
  public void testReuseWithExternalMark() {
    PayloadImpl p = new PayloadImpl(DATA_VAL, METADATA_VAL);
    assertDataAndMetadata(p, DATA_VAL, METADATA_VAL);
    p.getData().position(2).mark();
    assertDataAndMetadata(p, DATA_VAL, METADATA_VAL);
  }

  private void assertDataAndMetadata(Payload p, String dataVal, @Nullable String metadataVal) {
    assertThat("Unexpected data.", p.getDataUtf8(), equalTo(dataVal));
    if (metadataVal == null) {
      assertThat("Non-null metadata", p.hasMetadata(), equalTo(false));
    } else {
      assertThat("Null metadata", p.hasMetadata(), equalTo(true));
      assertThat("Unexpected metadata.", p.getMetadataUtf8(), equalTo(metadataVal));
    }
  }

  @Test
  public void staticMethods() {
    assertDataAndMetadata(textPayload(DATA_VAL, METADATA_VAL), DATA_VAL, METADATA_VAL);
    assertDataAndMetadata(textPayload(DATA_VAL), DATA_VAL, null);
  }
}
