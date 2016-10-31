/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.transport.tcp;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.reactivesocket.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class ReactiveSocketFrameLogger extends ChannelDuplexHandler {

    private final Logger logger;
    private final Level logLevel;

    public ReactiveSocketFrameLogger(String name, Level logLevel) {
        this.logLevel = logLevel;
        logger = LoggerFactory.getLogger(name);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        logFrameIfEnabled(ctx, msg, " Writing frame: ");
        super.write(ctx, msg, promise);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logFrameIfEnabled(ctx, msg, " Read frame: ");
        super.channelRead(ctx, msg);
    }

    private void logFrameIfEnabled(ChannelHandlerContext ctx, Object msg, String logMsgPrefix) {
        if (msg instanceof Frame) {
            Frame f = (Frame) msg;
            switch (logLevel) {
            case ERROR:
                if (logger.isErrorEnabled()) {
                    logger.error(ctx.channel() + logMsgPrefix + f);
                }
                break;
            case WARN:
                if (logger.isWarnEnabled()) {
                    logger.warn(ctx.channel() + logMsgPrefix + f);
                }
                break;
            case INFO:
                if (logger.isInfoEnabled()) {
                    logger.info(ctx.channel() + logMsgPrefix + f);
                }
                break;
            case DEBUG:
                if (logger.isDebugEnabled()) {
                    logger.debug(ctx.channel() + logMsgPrefix + f);
                }
                break;
            case TRACE:
                if (logger.isTraceEnabled()) {
                    logger.trace(ctx.channel() + logMsgPrefix + f);
                }
                break;
            }
        }
    }
}
