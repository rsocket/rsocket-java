package io.rsocket.core;

import static io.rsocket.core.StateUtils.REQUEST_MASK;
import static io.rsocket.core.StateUtils.SUBSCRIBED_FLAG;
import static io.rsocket.core.StateUtils.extractRequestN;

import java.util.HashMap;
import java.util.Map;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ErrorMessageFactory;

class ShouldNotHaveFlag extends BasicErrorMessageFactory {

  static final Map<Long, String> FLAGS_NAMES =
      new HashMap<Long, String>() {
        {
          put(StateUtils.UNSUBSCRIBED_STATE, "UNSUBSCRIBED");
          put(StateUtils.TERMINATED_STATE, "TERMINATED");
          put(SUBSCRIBED_FLAG, "SUBSCRIBED");
          put(StateUtils.REQUEST_MASK, "REQUESTED(%n)");
          put(StateUtils.FIRST_FRAME_SENT_FLAG, "FIRST_FRAME_SENT");
          put(StateUtils.REASSEMBLING_FLAG, "REASSEMBLING");
          put(StateUtils.INBOUND_TERMINATED_FLAG, "INBOUND_TERMINATED");
          put(StateUtils.OUTBOUND_TERMINATED_FLAG, "OUTBOUND_TERMINATED");
        }
      };

  static final String SHOULD_NOT_HAVE_FLAG =
      "Expected state\n\t%s\nto not have\n\t%s\nbut had\n\t[%s]";

  private ShouldNotHaveFlag(long currentState, long expectedFlag, String actualFlags) {
    super(
        SHOULD_NOT_HAVE_FLAG,
        toBinaryString(currentState),
        FLAGS_NAMES.get(expectedFlag),
        actualFlags);
  }

  static ErrorMessageFactory shouldNotHaveFlag(long currentState, long expectedFlag) {
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
    long requestN = extractRequestN(currentState);
    if (requestN > 0) {
      if (stringBuilder.length() > 0) {
        stringBuilder.append(", ");
      }
      stringBuilder.append(String.format(FLAGS_NAMES.get(REQUEST_MASK), requestN));
    }
    return new ShouldNotHaveFlag(currentState, expectedFlag, stringBuilder.toString());
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
