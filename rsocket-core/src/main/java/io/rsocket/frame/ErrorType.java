package io.rsocket.frame;

/**
 * The types of {@link Error} that can be set.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#error-codes">Error
 *     Codes</a>
 */
public final class ErrorType {

  /**
   * Application layer logic generating a Reactive Streams onError event. Stream ID MUST be &gt; 0.
   */
  public static final int APPLICATION_ERROR = 0x00000201;

  /**
   * The Responder canceled the request but may have started processing it (similar to REJECTED but
   * doesn't guarantee lack of side-effects). Stream ID MUST be &gt; 0.
   */
  public static final int CANCELED = 0x00000203;

  /**
   * The connection is being terminated. Stream ID MUST be 0. Sender or Receiver of this frame MUST
   * wait for outstanding streams to terminate before closing the connection. New requests MAY not
   * be accepted.
   */
  public static final int CONNECTION_CLOSE = 0x00000102;

  /**
   * The connection is being terminated. Stream ID MUST be 0. Sender or Receiver of this frame MAY
   * close the connection immediately without waiting for outstanding streams to terminate.
   */
  public static final int CONNECTION_ERROR = 0x00000101;

  /** The request is invalid. Stream ID MUST be &gt; 0. */
  public static final int INVALID = 0x00000204;

  /**
   * The Setup frame is invalid for the server (it could be that the client is too recent for the
   * old server). Stream ID MUST be 0.
   */
  public static final int INVALID_SETUP = 0x00000001;

  /**
   * Despite being a valid request, the Responder decided to reject it. The Responder guarantees
   * that it didn't process the request. The reason for the rejection is explained in the Error Data
   * section. Stream ID MUST be &gt; 0.
   */
  public static final int REJECTED = 0x00000202;

  /**
   * The server rejected the resume, it can specify the reason in the payload. Stream ID MUST be 0.
   */
  public static final int REJECTED_RESUME = 0x00000004;

  /**
   * The server rejected the setup, it can specify the reason in the payload. Stream ID MUST be 0.
   */
  public static final int REJECTED_SETUP = 0x00000003;

  /** Reserved. */
  public static final int RESERVED = 0x00000000;

  /** Reserved for Extension Use. */
  public static final int RESERVED_FOR_EXTENSION = 0xFFFFFFFF;

  /**
   * Some (or all) of the parameters specified by the client are unsupported by the server. Stream
   * ID MUST be 0.
   */
  public static final int UNSUPPORTED_SETUP = 0x00000002;

  /** Minimum allowed user defined error code value */
  public static final int MIN_USER_ALLOWED_ERROR_CODE = 0x00000301;

  /**
   * Maximum allowed user defined error code value. Note, the value is above signed integer maximum,
   * so it will be negative after overflow.
   */
  public static final int MAX_USER_ALLOWED_ERROR_CODE = 0xFFFFFFFE;

  private ErrorType() {}
}
