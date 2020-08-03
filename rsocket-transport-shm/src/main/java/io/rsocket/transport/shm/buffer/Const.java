package io.rsocket.transport.shm.buffer;

public interface Const {

  int _CACHELINE = 64;

  /** Position at which meta-data starts. */
  int META = 0;

  int META_MAX_PACKETS = META;

  int META_CAPACITY = META + 4;

  int FUTEX = META + _CACHELINE;

  /** Position at which the writer puts its sequence number. */
  int WSEQ = FUTEX + _CACHELINE;

  /** Position at which the reader puts its sequence number. */
  int RSEQ = WSEQ + _CACHELINE;

  int RESET = RSEQ + _CACHELINE;

  /** Position at which packet information starts. */
  int PACKET_INFO = RESET + _CACHELINE;

  /**
   * Length of data describing a packet. Two ints: position and length. Padded to cacheline size.
   */
  int LEN_PACKET_INFO = _CACHELINE;
}
