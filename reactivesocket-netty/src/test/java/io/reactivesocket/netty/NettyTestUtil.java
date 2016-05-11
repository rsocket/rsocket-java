/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.netty;

import io.netty.buffer.ByteBuf;

public class NettyTestUtil
{
    public static String byteBufToString(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        buf.readerIndex(readerIndex);

        StringBuilder result = new StringBuilder();
        StringBuilder ascii = new StringBuilder();
        int i = 0;
        for (i=0; i<bytes.length; i++) {
            byte b = bytes[i];
            result.append(String.format("%02X ", b));

            if (32 <= b && b < 127) {
                ascii.append((char)b);
            } else {
                ascii.append('.');
            }

            if ((i+1) % 16 == 0) {
                result.append("   ");
                result.append(ascii);
                result.append('\n');
                ascii = new StringBuilder();
            }
        }
        if ((bytes.length - 1) % 16 != 0) {
            int x = 16 - ((bytes.length - 1) % 16);
            StringBuilder padding = new StringBuilder();
            for (int j=0 ; j<x; j++) {
                result.append("   ");
            }
            result.append(ascii);
        }
        return result.toString();
    }
}
