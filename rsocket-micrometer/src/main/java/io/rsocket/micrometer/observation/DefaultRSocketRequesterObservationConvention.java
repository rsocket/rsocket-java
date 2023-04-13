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

import io.micrometer.common.KeyValues;
import io.micrometer.common.util.StringUtils;
import io.micrometer.observation.Observation;
import io.rsocket.frame.FrameType;

/**
 * Default {@link RSocketRequesterObservationConvention} implementation.
 *
 * @author Marcin Grzejszczak
 * @since 1.1.4
 */
public class DefaultRSocketRequesterObservationConvention
    extends DefaultRSocketObservationConvention implements RSocketRequesterObservationConvention {

  public DefaultRSocketRequesterObservationConvention(RSocketContext rSocketContext) {
    super(rSocketContext);
  }

  @Override
  public KeyValues getLowCardinalityKeyValues(RSocketContext context) {
    KeyValues values =
        KeyValues.of(
            RSocketObservationDocumentation.ResponderTags.REQUEST_TYPE.withValue(
                context.frameType.name()));
    if (StringUtils.isNotBlank(context.route)) {
      values =
          values.and(RSocketObservationDocumentation.ResponderTags.ROUTE.withValue(context.route));
    }
    return values;
  }

  @Override
  public boolean supportsContext(Observation.Context context) {
    return context instanceof RSocketContext;
  }

  @Override
  public String getName() {
    if (getRSocketContext().frameType == FrameType.REQUEST_RESPONSE) {
      return "rsocket.request";
    }
    return super.getName();
  }
}
