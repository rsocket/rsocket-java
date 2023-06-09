package io.rsocket.core;

import java.util.HashMap;
import java.util.Map;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ErrorMessageFactory;

import static io.rsocket.core.StateUtils.*;

class ShouldHaveFlag extends BasicErrorMessageFactory {

  static final Map<Long, String> FLAGS_NAMES =
      new HashMap<Long, String>() {
        {
          put(StateUtils.UNSUBSCRIBED_STATE, "UNSUBSCRIBED");
          put(StateUtils.TERMINATED_STATE, "TERMINATED");
          put(SUBSCRIBED_FLAG, "SUBSCRIBED");
          put(StateUtils.REQUEST_MASK, "REQUESTED(%s)");
          put(StateUtils.FIRST_FRAME_SENT_FLAG, "FIRST_FRAME_SENT");
          put(StateUtils.REASSEMBLING_FLAG, "REASSEMBLING");
          put(StateUtils.INBOUND_TERMINATED_FLAG, "INBOUND_TERMINATED");
          put(StateUtils.OUTBOUND_TERMINATED_FLAG, "OUTBOUND_TERMINATED");
        }
      };

  static final String SHOULD_HAVE_FLAG = "Expected state\n\t%s\nto have\n\t%s\nbut had\n\t[%s]";

  private ShouldHaveFlag(long currentState, String expectedFlag, String actualFlags) {
    super(SHOULD_HAVE_FLAG, toBinaryString(currentState), expectedFlag, actualFlags);
  }

  static ErrorMessageFactory shouldHaveFlag(long currentState, long expectedFlag) {
    String stateAsString = extractStateAsString(currentState);
    return new ShouldHaveFlag(currentState, FLAGS_NAMES.get(expectedFlag), stateAsString);
  }

  static ErrorMessageFactory shouldHaveRequestN(long currentState, long expectedRequestN) {
    String stateAsString = extractStateAsString(currentState);
    return new ShouldHaveFlag(
        currentState,
        String.format(
            FLAGS_NAMES.get(REQUEST_MASK),
            expectedRequestN == Integer.MAX_VALUE ? "MAX" : expectedRequestN),
        stateAsString);
  }

  static ErrorMessageFactory shouldHaveRequestNBetween(
      long currentState, long expectedRequestNMin, long expectedRequestNMax) {
    String stateAsString = extractStateAsString(currentState);
    return new ShouldHaveFlag(
        currentState,
        String.format(
            FLAGS_NAMES.get(REQUEST_MASK),
            (expectedRequestNMin == Integer.MAX_VALUE ? "MAX" : expectedRequestNMin)
                + " - "
                + (expectedRequestNMax == Integer.MAX_VALUE ? "MAX" : expectedRequestNMax)),
        stateAsString);
  }

  private static String extractStateAsString(long currentState) {
    StringBuilder stringBuilder = new StringBuilder();
    long flag = 1L << 31;
    for (int i = 0; i < 33; i++, flag <<= 1) {
      if ((currentState & flag) == flag) {
        if (stringBuilder.length() > 0) {
          stringBuilder.append(", ");
        }
        stringBuilder.append(FLAGS_NAMES.get(flag));
      }
    }
    long requestN = requestedTimes(currentState);
    if (requestN > 0) {
      if (stringBuilder.length() > 0) {
        stringBuilder.append(", ");
      }
      stringBuilder.append(
          String.format(
              FLAGS_NAMES.get(REQUEST_MASK), requestN >= Integer.MAX_VALUE ? "MAX" : requestN));
    }
    return stringBuilder.toString();
  }

  static String toBinaryString(long state) {
    StringBuilder binaryString = new StringBuilder(Long.toBinaryString(state));

    int diff = 64 - binaryString.length();
    for (int i = 0; i < diff; i++) {
      binaryString.insert(0, "0");
    }

    binaryString.insert(33, "_");
    binaryString.insert(0, "0b");

    return binaryString.toString();
  }
}
