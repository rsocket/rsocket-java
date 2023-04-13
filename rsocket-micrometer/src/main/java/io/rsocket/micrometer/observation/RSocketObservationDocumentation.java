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

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

enum RSocketObservationDocumentation implements ObservationDocumentation {

  /** Observation created on the RSocket responder side. */
  RSOCKET_RESPONDER {
    @Override
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultRSocketResponderObservationConvention.class;
    }
  },

  /** Observation created on the RSocket requester side for Fire and Forget frame type. */
  RSOCKET_REQUESTER_FNF {
    @Override
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultRSocketRequesterObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
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
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultRSocketResponderObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
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
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultRSocketRequesterObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
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
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultRSocketResponderObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
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
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultRSocketRequesterObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
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
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultRSocketResponderObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
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
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultRSocketRequesterObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
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
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultRSocketResponderObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
      return ResponderTags.values();
    }

    @Override
    public String getPrefix() {
      return "rsocket.";
    }
  };

  enum RequesterTags implements KeyName {

    /** Name of the RSocket route. */
    ROUTE {
      @Override
      public String asString() {
        return "rsocket.route";
      }
    },

    /** Name of the RSocket request type. */
    REQUEST_TYPE {
      @Override
      public String asString() {
        return "rsocket.request-type";
      }
    },

    /** Name of the RSocket content type. */
    CONTENT_TYPE {
      @Override
      public String asString() {
        return "rsocket.content-type";
      }
    }
  }

  enum ResponderTags implements KeyName {

    /** Name of the RSocket route. */
    ROUTE {
      @Override
      public String asString() {
        return "rsocket.route";
      }
    },

    /** Name of the RSocket request type. */
    REQUEST_TYPE {
      @Override
      public String asString() {
        return "rsocket.request-type";
      }
    }
  }
}
