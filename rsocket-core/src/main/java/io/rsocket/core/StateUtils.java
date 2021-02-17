package io.rsocket.core;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

final class StateUtils {

  /** Volatile Long Field bit mask that allows extract flags stored in the field */
  static final long FLAGS_MASK =
      0b111111111111111111111111111111111_0000000000000000000000000000000L;
  /** Volatile Long Field bit mask that allows extract int RequestN stored in the field */
  static final long REQUEST_MASK =
      0b000000000000000000000000000000000_1111111111111111111111111111111L;
  /** Bit Flag that indicates Requester Producer has been subscribed once */
  static final long SUBSCRIBED_FLAG =
      0b000000000000000000000000000000001_0000000000000000000000000000000L;
  /** Bit Flag that indicates that the first payload in RequestChannel scenario is received */
  static final long FIRST_PAYLOAD_RECEIVED_FLAG =
      0b000000000000000000000000000000010_0000000000000000000000000000000L;
  /**
   * Bit Flag that indicates that the logical stream is prepared for the first initial frame sending
   * (applicable for requester only)
   */
  static final long PREPARED_FLAG =
      0b000000000000000000000000000000100_0000000000000000000000000000000L;
  /**
   * Bit Flag that indicates that sent first initial frame was sent (in case of requester) or
   * consumed (if responder)
   */
  static final long FIRST_FRAME_SENT_FLAG =
      0b000000000000000000000000000001000_0000000000000000000000000000000L;
  /** Bit Flag that indicates that there is a frame being reassembled */
  static final long REASSEMBLING_FLAG =
      0b000000000000000000000000000010000_0000000000000000000000000000000L;
  /**
   * Bit Flag that indicates requestChannel stream is half terminated. In this case flag indicates
   * that the inbound is terminated
   */
  static final long INBOUND_TERMINATED_FLAG =
      0b000000000000000000000000000100000_0000000000000000000000000000000L;
  /**
   * Bit Flag that indicates requestChannel stream is half terminated. In this case flag indicates
   * that the outbound is terminated
   */
  static final long OUTBOUND_TERMINATED_FLAG =
      0b000000000000000000000000001000000_0000000000000000000000000000000L;
  /** Initial state for any request operator */
  static final long UNSUBSCRIBED_STATE =
      0b000000000000000000000000000000000_0000000000000000000000000000000L;
  /** State that indicates request operator was terminated */
  static final long TERMINATED_STATE =
      0b100000000000000000000000000000000_0000000000000000000000000000000L;

  /**
   * Adds (if possible) to the given state the {@link #SUBSCRIBED_FLAG} flag which indicates that
   * the given stream has already been subscribed once
   *
   * <p>Note, the flag will not be added if the stream has already been terminated or if the stream
   * has already been subscribed once
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markSubscribed(AtomicLongFieldUpdater<T> updater, T instance) {
    return markSubscribed(updater, instance, false);
  }

  /**
   * Adds (if possible) to the given state the {@link #SUBSCRIBED_FLAG} flag which indicates that
   * the given stream has already been subscribed once
   *
   * <p>Note, the flag will not be added if the stream has already been terminated or if the stream
   * has already been subscribed once
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param markPrepared indicates whether the given instance should be marked as prepared
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markSubscribed(
      AtomicLongFieldUpdater<T> updater, T instance, boolean markPrepared) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & SUBSCRIBED_FLAG) == SUBSCRIBED_FLAG) {
        return state;
      }

      if (updater.compareAndSet(
          instance, state, state | SUBSCRIBED_FLAG | (markPrepared ? PREPARED_FLAG : 0))) {
        return state;
      }
    }
  }

  /**
   * Indicates that the given stream has already been subscribed once
   *
   * @param state to check whether stream is subscribed
   * @return true if the {@link #SUBSCRIBED_FLAG} flag is set
   */
  static boolean isSubscribed(long state) {
    return (state & SUBSCRIBED_FLAG) == SUBSCRIBED_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #FIRST_FRAME_SENT_FLAG} flag which indicates
   * that the first frame has already set and logical stream has already been established.
   *
   * <p>Note, the flag will not be added if the stream has already been terminated or if the stream
   * has already been established once
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markFirstFrameSent(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & FIRST_FRAME_SENT_FLAG) == FIRST_FRAME_SENT_FLAG) {
        return state;
      }

      if (updater.compareAndSet(instance, state, state | FIRST_FRAME_SENT_FLAG)) {
        return state;
      }
    }
  }

  /**
   * Indicates that the first frame which established logical stream has already been sent
   *
   * @param state to check whether stream is established
   * @return true if the {@link #FIRST_FRAME_SENT_FLAG} flag is set
   */
  static boolean isFirstFrameSent(long state) {
    return (state & FIRST_FRAME_SENT_FLAG) == FIRST_FRAME_SENT_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #PREPARED_FLAG} flag which indicates that the
   * logical stream is ready for initial frame sending.
   *
   * <p>Note, the flag will not be added if the stream has already been terminated or if the stream
   * has already been marked as prepared
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markPrepared(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & PREPARED_FLAG) == PREPARED_FLAG) {
        return state;
      }

      if (updater.compareAndSet(instance, state, state | PREPARED_FLAG)) {
        return state;
      }
    }
  }

  /**
   * Indicates that the logical stream is ready for initial frame sending
   *
   * @param state to check whether stream is prepared for initial frame sending
   * @return true if the {@link #FIRST_PAYLOAD_RECEIVED_FLAG} flag is set
   */
  static boolean isPrepared(long state) {
    return (state & PREPARED_FLAG) == PREPARED_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #FIRST_PAYLOAD_RECEIVED_FLAG} flag which
   * indicates that the logical stream is ready for initial frame sending.
   *
   * <p>Note, the flag will not be added if the stream has already been terminated or if the stream
   * has already been marked as prepared
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markFirstPayloadReceived(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & FIRST_PAYLOAD_RECEIVED_FLAG) == FIRST_PAYLOAD_RECEIVED_FLAG) {
        return state;
      }

      if (updater.compareAndSet(instance, state, state | FIRST_PAYLOAD_RECEIVED_FLAG)) {
        return state;
      }
    }
  }

  /**
   * Indicates that the logical stream is ready for initial frame sending
   *
   * @param state to check whether stream is established
   * @return true if the {@link #FIRST_PAYLOAD_RECEIVED_FLAG} flag is set
   */
  static boolean isFirstPayloadReceived(long state) {
    return (state & FIRST_PAYLOAD_RECEIVED_FLAG) == FIRST_PAYLOAD_RECEIVED_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #REASSEMBLING_FLAG} flag which indicates that
   * there is a payload reassembling in progress.
   *
   * <p>Note, the flag will not be added if the stream has already been terminated
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markReassembling(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if (updater.compareAndSet(instance, state, state | REASSEMBLING_FLAG)) {
        return state;
      }
    }
  }

  /**
   * Removes (if possible) from the given state the {@link #REASSEMBLING_FLAG} flag which indicates
   * that a payload reassembly process is completed.
   *
   * <p>Note, the flag will not be removed if the stream has already been terminated
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markReassembled(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if (updater.compareAndSet(instance, state, state & ~REASSEMBLING_FLAG)) {
        return state;
      }
    }
  }

  /**
   * Indicates that a payload reassembly process is completed.
   *
   * @param state to check whether there is reassembly in progress
   * @return true if the {@link #REASSEMBLING_FLAG} flag is set
   */
  static boolean isReassembling(long state) {
    return (state & REASSEMBLING_FLAG) == REASSEMBLING_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #INBOUND_TERMINATED_FLAG} flag which indicates
   * that an inbound channel of a bidirectional stream is terminated.
   *
   * <p><b>Note</b>, this action will have no effect if the stream has already been terminated or if
   * the {@link #INBOUND_TERMINATED_FLAG} flag has already been set. <br>
   * <b>Note</b>, if the outbound stream has already been terminated, then the result state will be
   * {@link #TERMINATED_STATE}
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markInboundTerminated(AtomicLongFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & INBOUND_TERMINATED_FLAG) == INBOUND_TERMINATED_FLAG) {
        return state;
      }

      if ((state & OUTBOUND_TERMINATED_FLAG) == OUTBOUND_TERMINATED_FLAG) {
        if (updater.compareAndSet(instance, state, TERMINATED_STATE)) {
          return state;
        }
      } else {
        if (updater.compareAndSet(instance, state, state | INBOUND_TERMINATED_FLAG)) {
          return state;
        }
      }
    }
  }

  /**
   * Indicates that a the inbound channel of a bidirectional stream is terminated.
   *
   * @param state to check whether it has {@link #INBOUND_TERMINATED_FLAG} set
   * @return true if the {@link #INBOUND_TERMINATED_FLAG} flag is set
   */
  static boolean isInboundTerminated(long state) {
    return (state & INBOUND_TERMINATED_FLAG) == INBOUND_TERMINATED_FLAG;
  }

  /**
   * Adds (if possible) to the given state the {@link #OUTBOUND_TERMINATED_FLAG} flag which
   * indicates that an outbound channel of a bidirectional stream is terminated.
   *
   * <p><b>Note</b>, this action will have no effect if the stream has already been terminated or if
   * the {@link #OUTBOUND_TERMINATED_FLAG} flag has already been set. <br>
   * <b>Note</b>, if the {@code checkEstablishment} parameter is {@code true} and the logical stream
   * is not established, then the result state will be {@link #TERMINATED_STATE} <br>
   * <b>Note</b>, if the inbound stream has already been terminated, then the result state will be
   * {@link #TERMINATED_STATE}
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param checkEstablishment indicates whether {@link #FIRST_FRAME_SENT_FLAG} should be checked to
   *     make final decision
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markOutboundTerminated(
      AtomicLongFieldUpdater<T> updater, T instance, boolean checkEstablishment) {
    for (; ; ) {
      long state = updater.get(instance);

      if (state == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      if ((state & OUTBOUND_TERMINATED_FLAG) == OUTBOUND_TERMINATED_FLAG) {
        return state;
      }

      if ((checkEstablishment && !isFirstFrameSent(state))
          || (state & INBOUND_TERMINATED_FLAG) == INBOUND_TERMINATED_FLAG) {
        if (updater.compareAndSet(instance, state, TERMINATED_STATE)) {
          return state;
        }
      } else {
        if (updater.compareAndSet(instance, state, state | OUTBOUND_TERMINATED_FLAG)) {
          return state;
        }
      }
    }
  }

  /**
   * Indicates that a the outbound channel of a bidirectional stream is terminated.
   *
   * @param state to check whether it has {@link #OUTBOUND_TERMINATED_FLAG} set
   * @return true if the {@link #OUTBOUND_TERMINATED_FLAG} flag is set
   */
  static boolean isOutboundTerminated(long state) {
    return (state & OUTBOUND_TERMINATED_FLAG) == OUTBOUND_TERMINATED_FLAG;
  }

  /**
   * Makes current state a {@link #TERMINATED_STATE}
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   * @return return previous state before setting the new one
   */
  static <T> long markTerminated(AtomicLongFieldUpdater<T> updater, T instance) {
    return updater.getAndSet(instance, TERMINATED_STATE);
  }

  /**
   * Makes current state a {@link #TERMINATED_STATE} using {@link
   * AtomicLongFieldUpdater#lazySet(Object, long)}
   *
   * @param updater of the volatile state field
   * @param instance instance holder of the volatile state
   * @param <T> generic type of the instance
   */
  static <T> void lazyTerminate(AtomicLongFieldUpdater<T> updater, T instance) {
    updater.lazySet(instance, TERMINATED_STATE);
  }

  /**
   * Indicates that a the outbound channel of a bidirectional stream is terminated.
   *
   * @param state to check whether it has {@link #OUTBOUND_TERMINATED_FLAG} set
   * @return true if the {@link #OUTBOUND_TERMINATED_FLAG} flag is set
   */
  static boolean isTerminated(long state) {
    return state == TERMINATED_STATE;
  }

  /**
   * Shortcut for {@link #isSubscribed} {@code ||} {@link #isTerminated} methods
   *
   * @param state to check flags on
   * @return true if state is terminated or has flag subscribed
   */
  static boolean isSubscribedOrTerminated(long state) {
    return state == TERMINATED_STATE || (state & SUBSCRIBED_FLAG) == SUBSCRIBED_FLAG;
  }

  static <T> long addRequestN(AtomicLongFieldUpdater<T> updater, T instance, long toAdd) {
    return addRequestN(updater, instance, toAdd, false);
  }

  static <T> long addRequestN(
      AtomicLongFieldUpdater<T> updater, T instance, long toAdd, boolean markPrepared) {
    long currentState, flags, requestN, nextRequestN;
    for (; ; ) {
      currentState = updater.get(instance);

      if (currentState == TERMINATED_STATE) {
        return TERMINATED_STATE;
      }

      requestN = currentState & REQUEST_MASK;
      if (requestN == REQUEST_MASK) {
        return currentState;
      }

      flags = (currentState & FLAGS_MASK) | (markPrepared ? PREPARED_FLAG : 0);
      nextRequestN = addRequestN(requestN, toAdd);

      if (updater.compareAndSet(instance, currentState, nextRequestN | flags)) {
        return currentState;
      }
    }
  }

  static long addRequestN(long a, long b) {
    long res = a + b;
    if (res < 0 || res > REQUEST_MASK) {
      return REQUEST_MASK;
    }
    return res;
  }

  static boolean hasRequested(long state) {
    return (state & REQUEST_MASK) > 0;
  }

  static long extractRequestN(long state) {
    long requestN = state & REQUEST_MASK;

    if (requestN == REQUEST_MASK) {
      return REQUEST_MASK;
    }

    return requestN;
  }

  static boolean isMaxAllowedRequestN(long n) {
    return n >= REQUEST_MASK;
  }
}
