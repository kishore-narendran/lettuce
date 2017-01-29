/*
 * Copyright 2011-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lambdaworks.redis.masterslave;

import java.util.*;

import com.lambdaworks.redis.ReadFrom;
import com.lambdaworks.redis.RedisChannelWriter;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.internal.LettuceAssert;
import com.lambdaworks.redis.protocol.ConnectionFacade;
import com.lambdaworks.redis.protocol.ProtocolKeyword;
import com.lambdaworks.redis.protocol.RedisCommand;

/**
 * Channel writer/dispatcher that dispatches commands based on the intent to different connections.
 *
 * @author Mark Paluch
 */
class MasterSlaveChannelWriter<K, V> implements RedisChannelWriter {

    private MasterSlaveConnectionProvider<K, V> masterSlaveConnectionProvider;
    private boolean closed = false;

    public MasterSlaveChannelWriter(MasterSlaveConnectionProvider<K, V> masterSlaveConnectionProvider) {
        this.masterSlaveConnectionProvider = masterSlaveConnectionProvider;
    }

    @Override
    public <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command) {

        LettuceAssert.notNull(command, "Command must not be null");

        if (closed) {
            throw new RedisException("Connection is closed");
        }

        MasterSlaveConnectionProvider.Intent intent = getIntent(command.getType());
        StatefulRedisConnection<K, V> connection = (StatefulRedisConnection) masterSlaveConnectionProvider
                .getConnection(intent);

        return connection.dispatch(command);
    }

    @Override
    public <K, V> Collection<RedisCommand<K, V, ?>> write(Collection<? extends RedisCommand<K, V, ?>> commands) {

        LettuceAssert.notNull(commands, "Commands must not be null");

        if (closed) {
            throw new RedisException("Connection is closed");
        }

        List<RedisCommand<K, V, ?>> read = new ArrayList<>(commands.size());
        List<RedisCommand<K, V, ?>> write = new ArrayList<>(commands.size());

        // TODO: Retain order or retain Intent preference?
        // Currently: Retain order
        MasterSlaveConnectionProvider.Intent intent = getIntent(commands);

        for (RedisCommand<K, V, ?> command : commands) {

            if (intent == MasterSlaveConnectionProvider.Intent.READ) {
                read.add(command);
            } else {
                write.add(command);
            }
        }
        StatefulRedisConnection<K, V> readConnection = null;
        StatefulRedisConnection<K, V> writeConnection = null;

        if (!read.isEmpty()) {
            readConnection = (StatefulRedisConnection) masterSlaveConnectionProvider
                    .getConnection(MasterSlaveConnectionProvider.Intent.READ);
            readConnection.dispatch(read);
        }

        if (!write.isEmpty()) {
            writeConnection = (StatefulRedisConnection) masterSlaveConnectionProvider
                    .getConnection(MasterSlaveConnectionProvider.Intent.WRITE);
            writeConnection.dispatch(write);
        }

        read.addAll(write);

        return read;
    }

    /**
     * Optimization: Determine command intents and optimize for bulk execution preferring one node.
     * <p>
     * If there is only one intent, then we take the intent derived from the commands. If there is more than one intent, then
     * use {@link MasterSlaveConnectionProvider.Intent#WRITE}.
     *
     * @param commands {@link Collection} of {@link RedisCommand commands}.
     * @return the intent.
     */
    static MasterSlaveConnectionProvider.Intent getIntent(Collection<? extends RedisCommand<?, ?, ?>> commands) {

        Set<MasterSlaveConnectionProvider.Intent> intents = new HashSet<>(2, 1);
        MasterSlaveConnectionProvider.Intent singleIntent = MasterSlaveConnectionProvider.Intent.WRITE;

        for (RedisCommand<?, ?, ?> command : commands) {

            singleIntent = getIntent(command.getType());
            intents.add(singleIntent);

            if (intents.size() > 1) {
                return MasterSlaveConnectionProvider.Intent.WRITE;
            }
        }

        return singleIntent;
    }

    private static MasterSlaveConnectionProvider.Intent getIntent(ProtocolKeyword type) {
        return ReadOnlyCommands.isReadOnlyCommand(type) ? MasterSlaveConnectionProvider.Intent.READ
                : MasterSlaveConnectionProvider.Intent.WRITE;
    }

    @Override
    public void close() {

        if (closed) {
            return;
        }

        closed = true;

        if (masterSlaveConnectionProvider != null) {
            masterSlaveConnectionProvider.close();
            masterSlaveConnectionProvider = null;
        }
    }

    public MasterSlaveConnectionProvider getMasterSlaveConnectionProvider() {
        return masterSlaveConnectionProvider;
    }

    @Override
    public void setConnectionFacade(ConnectionFacade connection) {

    }

    @Override
    public void setAutoFlushCommands(boolean autoFlush) {
        masterSlaveConnectionProvider.setAutoFlushCommands(autoFlush);
    }

    @Override
    public void flushCommands() {
        masterSlaveConnectionProvider.flushCommands();
    }

    @Override
    public void reset() {
        masterSlaveConnectionProvider.reset();
    }

    /**
     * Set from which nodes data is read. The setting is used as default for read operations on this connection. See the
     * documentation for {@link ReadFrom} for more information.
     *
     * @param readFrom the read from setting, must not be {@literal null}
     */
    public void setReadFrom(ReadFrom readFrom) {
        masterSlaveConnectionProvider.setReadFrom(readFrom);
    }

    /**
     * Gets the {@link ReadFrom} setting for this connection. Defaults to {@link ReadFrom#MASTER} if not set.
     *
     * @return the read from setting
     */
    public ReadFrom getReadFrom() {
        return masterSlaveConnectionProvider.getReadFrom();
    }

}
