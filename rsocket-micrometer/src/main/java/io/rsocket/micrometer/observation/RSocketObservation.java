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

import io.micrometer.api.instrument.docs.DocumentedObservation;
import io.micrometer.api.instrument.docs.TagKey;

enum RSocketObservation implements DocumentedObservation {

  /** Observation created on the RSocket responder side. */
  RSOCKET_RESPONDER {
    @Override
    public String getName() {
      return "%s";
    }
  },

  /** Observation created on the RSocket requester side for Fire and Forget frame type. */
  RSOCKET_REQUESTER_FNF {
    @Override
    public String getName() {
      return "rsocket.fnf";
    }

    @Override
    public TagKey[] getLowCardinalityTagKeys() {
      return RequesterTags.values();
    }

    @Override
    public String getPrefix() {
      return "rsocket.";
    }
  },

  /** Observation created on the RSocket responder side for Fire and Forget frame type. */
  RSOCKET_RESPONDER_FNF {
    @Override
    public String getName() {
      return "rsocket.fnf";
    }

    @Override
    public TagKey[] getLowCardinalityTagKeys() {
      return ResponderTags.values();
    }

    @Override
    public String getPrefix() {
      return "rsocket.";
    }
  },

  /** Observation created on the RSocket requester side for Request Response frame type. */
  RSOCKET_REQUESTER_REQUEST_RESPONSE {
    @Override
    public String getName() {
      return "rsocket.request";
    }

    @Override
    public TagKey[] getLowCardinalityTagKeys() {
      return RequesterTags.values();
    }

    @Override
    public String getPrefix() {
      return "rsocket.";
    }
  },

  /** Observation created on the RSocket responder side for Request Response frame type. */
  RSOCKET_RESPONDER_REQUEST_RESPONSE {
    @Override
    public String getName() {
      return "rsocket.response";
    }

    @Override
    public TagKey[] getLowCardinalityTagKeys() {
      return ResponderTags.values();
    }

    @Override
    public String getPrefix() {
      return "rsocket.";
    }
  },

  /** Observation created on the RSocket requester side for Request Stream frame type. */
  RSOCKET_REQUESTER_REQUEST_STREAM {
    @Override
    public String getName() {
      return "rsocket.stream";
    }

    @Override
    public TagKey[] getLowCardinalityTagKeys() {
      return RequesterTags.values();
    }

    @Override
    public String getPrefix() {
      return "rsocket.";
    }
  },

  /** Observation created on the RSocket responder side for Request Stream frame type. */
  RSOCKET_RESPONDER_REQUEST_STREAM {
    @Override
    public String getName() {
      return "rsocket.stream";
    }

    @Override
    public TagKey[] getLowCardinalityTagKeys() {
      return ResponderTags.values();
    }

    @Override
    public String getPrefix() {
      return "rsocket.";
    }
  },

  /** Observation created on the RSocket requester side for Request Channel frame type. */
  RSOCKET_REQUESTER_REQUEST_CHANNEL {
    @Override
    public String getName() {
      return "rsocket.channel";
    }

    @Override
    public TagKey[] getLowCardinalityTagKeys() {
      return RequesterTags.values();
    }

    @Override
    public String getPrefix() {
      return "rsocket.";
    }
  },

  /** Observation created on the RSocket responder side for Request Channel frame type. */
  RSOCKET_RESPONDER_REQUEST_CHANNEL {
    @Override
    public String getName() {
      return "rsocket.channel";
    }

    @Override
    public TagKey[] getLowCardinalityTagKeys() {
      return ResponderTags.values();
    }

    @Override
    public String getPrefix() {
      return "rsocket.";
    }
  };

  enum RequesterTags implements TagKey {

    /** Name of the RSocket route. */
    ROUTE {
      @Override
      public String getKey() {
        return "rsocket.route";
      }
    },

    /** Name of the RSocket request type. */
    REQUEST_TYPE {
      @Override
      public String getKey() {
        return "rsocket.request-type";
      }
    },

    /** Name of the RSocket content type. */
    CONTENT_TYPE {
      @Override
      public String getKey() {
        return "rsocket.content-type";
      }
    }
  }

  enum ResponderTags implements TagKey {

    /** Name of the RSocket route. */
    ROUTE {
      @Override
      public String getKey() {
        return "rsocket.route";
      }
    },

    /** Name of the RSocket request type. */
    REQUEST_TYPE {
      @Override
      public String getKey() {
        return "rsocket.request-type";
      }
    }
  }
}
