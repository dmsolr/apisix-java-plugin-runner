/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.apisix.plugin.runner.server;

import io.netty.channel.ChannelHandler;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.logging.LoggingHandler;
import org.apache.apisix.plugin.runner.handler.ServerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

import java.util.Objects;

@Component
public class NettyUnixDomainSocketServer implements UnixDomainSocketServer {
    
    private static final Logger logger = LoggerFactory.getLogger(NettyUnixDomainSocketServer.class);
    
    private DisposableServer server;
    
    @Value("${socket.file:/tmp/runner.socks}")
    private String socketFile;
    
    @Autowired(required = false)
    private ChannelHandler handler;
    
    @Autowired
    private ServerHandler serverHandler;
    
    @Override
    public void start() {
        TcpServer server = TcpServer.create()
                .bindAddress(() -> new DomainSocketAddress(socketFile))
                .doOnChannelInit((observer, channel, addr) -> {
                    if (Objects.nonNull(handler)) {
                        channel.pipeline().addFirst(handler);
                        channel.pipeline().addFirst("logger", new LoggingHandler());
                    }
                });
        
        if (Objects.nonNull(serverHandler)) {
            server = server.handle((in, out) -> serverHandler.handler(in, out));
        }
        this.server = server.bindNow();
        
        Thread awaitThread = new Thread(() -> {
            this.server.onDispose().block();
        });
        awaitThread.setDaemon(false);
        awaitThread.setName("uds-server");
        awaitThread.start();
    }
    
    @Override
    public void stop() {
        server.disposeNow();
    }
    
}
