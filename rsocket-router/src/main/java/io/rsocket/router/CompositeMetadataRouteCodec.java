/*
 * Copyright 2015-Present the original author or authors.
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

package io.rsocket.router;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.metadata.CompositeMetadata.Entry;
import io.rsocket.metadata.CompositeMetadata.WellKnownMimeTypeEntry;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.WellKnownMimeType;
import reactor.util.annotation.Nullable;

public class CompositeMetadataRouteCodec implements RouteCodec {

  @Override
  @Nullable
  public Route decode(ByteBuf metadataByteBuf, FrameType requestType) {
    final CompositeMetadata compositeMetadata = new CompositeMetadata(metadataByteBuf, false);

    String route = null;
    String mimeType = null;

    for (Entry compositeMetadatum : compositeMetadata) {
      if (compositeMetadatum instanceof WellKnownMimeTypeEntry) {
        final WellKnownMimeTypeEntry wellKnownMimeTypeEntry =
            (WellKnownMimeTypeEntry) compositeMetadatum;
        final WellKnownMimeType type = wellKnownMimeTypeEntry.getType();

        if (type == WellKnownMimeType.MESSAGE_RSOCKET_ROUTING) {
          final RoutingMetadata routingMetadata =
              new RoutingMetadata(compositeMetadatum.getContent());
          for (String routeEntry : routingMetadata) {
            route = routeEntry;
            break;
          }
        } else if (type == WellKnownMimeType.MESSAGE_RSOCKET_MIMETYPE) {
          // FIXME: once codecs are available
          // mimeType = compositeMetadatum
        }
      }
    }

    if (route != null) {
      return new Route(requestType, route, mimeType);
    }

    return null;
  }
}
