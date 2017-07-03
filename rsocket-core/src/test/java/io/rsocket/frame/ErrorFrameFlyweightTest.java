/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.frame;

import static io.rsocket.frame.ErrorFrameFlyweight.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;

import io.rsocket.Frame;
import io.rsocket.exceptions.*;
import org.junit.Test;

public class ErrorFrameFlyweightTest {
  private final ByteBuf byteBuf = Unpooled.buffer(1024);

  @Test
  public void testEncoding() {
    int encoded =
        ErrorFrameFlyweight.encode(
            byteBuf,
            1,
            ErrorFrameFlyweight.APPLICATION_ERROR,
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    assertEquals(
        "00000b000000012c000000020164", ByteBufUtil.hexDump(byteBuf, 0, encoded));

    assertEquals(ErrorFrameFlyweight.APPLICATION_ERROR, ErrorFrameFlyweight.errorCode(byteBuf));
    assertEquals("d", ErrorFrameFlyweight.message(byteBuf));
  }

  @Test
  public void testExceptions() throws Exception{
    assertExceptionMapping(INVALID_SETUP, InvalidSetupException.class);
    assertExceptionMapping(UNSUPPORTED_SETUP, UnsupportedSetupException.class);
    assertExceptionMapping(REJECTED_SETUP, RejectedSetupException.class);
    assertExceptionMapping(REJECTED_RESUME, RejectedResumeException.class);
    assertExceptionMapping(CONNECTION_ERROR, ConnectionException.class);
    assertExceptionMapping(CONNECTION_CLOSE, ConnectionCloseException.class);
    assertExceptionMapping(APPLICATION_ERROR, ApplicationException.class);
    assertExceptionMapping(REJECTED, RejectedException.class);
    assertExceptionMapping(CANCELED, CancelException.class);
    assertExceptionMapping(INVALID, InvalidRequestException.class);
  }

  private <T extends Exception> void assertExceptionMapping(int errorCode, Class<T> exceptionClass) throws Exception {
    T ex = exceptionClass.getConstructor(String.class).newInstance("error data");
    Frame f = Frame.Error.from(0, ex);

    assertEquals(errorCode, Frame.Error.errorCode(f));

    RuntimeException ex2 = Exceptions.from(f);

    assertEquals(ex.getMessage(), ex2.getMessage());
    assertTrue(exceptionClass.isInstance(ex2));
  }


}
