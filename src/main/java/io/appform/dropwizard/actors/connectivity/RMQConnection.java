
/*
 * Copyright (c) 2019 Santanu Sinha <santanu.sinha@gmail.com>
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
 *  limitations under the License.
 */

package io.appform.dropwizard.actors.connectivity;

import com.codahale.metrics.health.HealthCheck;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.StandardMetricsCollector;
import io.appform.dropwizard.actors.TtlConfig;
import io.appform.dropwizard.actors.actor.ActorConfig;
import io.appform.dropwizard.actors.base.ConnectionMeta;
import io.appform.dropwizard.actors.base.utils.ConsumerMeta;
import io.appform.dropwizard.actors.base.utils.NamingUtils;
import io.appform.dropwizard.actors.common.ErrorCode;
import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.UUID;

@Slf4j
public class RMQConnection implements Managed {
    @Getter
    private final RMQConfig config;
    private final String name;
    private Connection connection;
    private Channel channel;
    private final ExecutorService executorService;
    private final Environment environment;
    private TtlConfig ttlConfig;
    private ConnectionShutdownRoutine connectionShutdownRoutine;

    @Getter
    private ConnectionMeta<String> connectionMeta;

    private static final Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
            .retryIfExceptionOfType(RabbitmqActorException.class)
            .withStopStrategy(StopStrategies.stopAfterAttempt(3))
            .withWaitStrategy(WaitStrategies.fixedWait(3, TimeUnit.SECONDS))
            .build();

    public RMQConnection(final String name,
                         final RMQConfig config,
                         final ExecutorService executorService,
                         final Environment environment,
                         final TtlConfig ttlConfig) {
        this.name = name;
        this.config = config;
        this.executorService = executorService;
        this.environment = environment;
        this.ttlConfig = ttlConfig;
        this.connectionMeta = new ConnectionMeta<>();
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("Starting RMQ connection [%s]", name));
        ConnectionFactory factory = new ConnectionFactory();
        factory.setMetricsCollector(new StandardMetricsCollector(environment.metrics(), metricPrefix(name)));
        if (config.isSecure()) {
            factory.setUsername(config.getUserName());
            factory.setPassword(config.getPassword());
            if (Strings.isNullOrEmpty(config.getCertStorePath())) {
                factory.useSslProtocol();
            } else {
                Preconditions.checkNotNull(config.getCertPassword(), "Cert password is required if cert file path has been provided");
                KeyStore ks = KeyStore.getInstance("JKS");
                ks.load(new FileInputStream(config.getCertStorePath()), config.getCertPassword().toCharArray());

                KeyStore tks = KeyStore.getInstance("JKS");
                tks.load(new FileInputStream(config.getServerCertStorePath()), config.getServerCertPassword().toCharArray());
                SSLContext c = SSLContexts.custom()
                        .useProtocol("TLSv1.2")
                        .loadTrustMaterial(tks, new TrustSelfSignedStrategy())
                        .loadKeyMaterial(ks, config.getCertPassword().toCharArray(), (aliases, socket) -> "clientcert")
                        .build();
                factory.useSslProtocol(c);
                factory.setVirtualHost(config.getUserName());
            }
        } else {
            factory.setUsername(config.getUserName());
            factory.setPassword(config.getPassword());
        }
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(3000);
        factory.setRequestedHeartbeat(60);
        if (!Strings.isNullOrEmpty(config.getVirtualHost())) {
            factory.setVirtualHost(config.getVirtualHost());
        }
        connection = factory.newConnection(executorService,
                config.getBrokers().stream()
                        .map(broker -> new Address(broker.getHost(), broker.getPort()))
                        .toArray(Address[]::new)
        );
        connection.addBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) {
                log.warn(String.format("RMQ Connection [%s] is blocked due to [%s]", name, reason));
            }

            @Override
            public void handleUnblocked() {
                log.warn(String.format("RMQ Connection [%s] is unblocked now", name));
            }
        });
        channel = connection.createChannel();
        environment.healthChecks().register(String.format("rmqconnection-%s-%s", connection, UUID.randomUUID().toString()), healthcheck());
        log.info(String.format("Started RMQ connection [%s] ", name));
    }

    private String metricPrefix(String name) {
        return String.format("rmqconnection.%s", NamingUtils.sanitizeMetricName(name));
    }

    public void ensure(final String queueName,
                       final String exchange,
                       final Map<String, Object> rmqOpts) throws Exception {
        ensure(queueName, queueName, exchange, rmqOpts);
    }

    public void ensure(final String queueName,
                       final String routingQueue,
                       final String exchange,
                       final Map<String, Object> rmqOpts) throws Exception {
        channel.queueDeclare(queueName, true, false, false, rmqOpts);
        channel.queueBind(queueName, exchange, routingQueue);
        log.info("Created queue: {} bound to {}", queueName, exchange);
    }

    public Map<String, Object> rmqOpts(final ActorConfig actorConfig) {
        final Map<String, Object> ttlOpts = getActorTTLOpts(actorConfig.getTtlConfig());
        return ImmutableMap.<String, Object>builder()
                .putAll(ttlOpts)
                .put("x-ha-policy", "all")
                .put("ha-mode", "all")
                .build();
    }

    public Map<String, Object> rmqOpts(final String deadLetterExchange,
                                       final ActorConfig actorConfig) {
        final Map<String, Object> ttlOpts = getActorTTLOpts(actorConfig.getTtlConfig());
        return ImmutableMap.<String, Object>builder()
                .putAll(ttlOpts)
                .put("x-ha-policy", "all")
                .put("ha-mode", "all")
                .put("x-dead-letter-exchange", deadLetterExchange)
                .build();
    }

    public HealthCheck healthcheck() {
        return new HealthCheck() {
            @Override
            protected Result check() {
                if (connection == null) {
                    log.warn("RMQ Healthcheck::No RMQ connection available");
                    return Result.unhealthy("No RMQ connection available");
                }
                if (!connection.isOpen()) {
                    log.warn("RMQ Healthcheck::RMQ connection is not open");
                    return Result.unhealthy("RMQ connection is not open");
                }
                if (null == channel) {
                    log.warn("RMQ Healthcheck::Producer channel is down");
                    return Result.unhealthy("Producer channel is down");
                }
                if (!channel.isOpen()) {
                    log.warn("RMQ Healthcheck::Producer channel is closed");
                    return Result.unhealthy("Producer channel is closed");
                }
                return Result.healthy();
            }
        };
    }

    @Override
    public void stop() {
    }

    public Channel channel() {
        return channel;
    }

    public Channel newChannel() throws IOException {
        return connection.createChannel();
    }

    private String getSideline(String name) {
        return String.format("%s_%s", name, "SIDELINE");
    }

    private Map<String, Object> getActorTTLOpts(final TtlConfig ttlConfig) {
        if (ttlConfig != null) {
            return getTTLOpts(ttlConfig);
        }
        return getTTLOpts(this.ttlConfig);
    }

    private Map<String, Object> getTTLOpts(final TtlConfig ttlConfig) {
        final Map<String, Object> ttlOpts = new HashMap<>();
        if (ttlConfig != null && ttlConfig.isTtlEnabled()) {
            ttlOpts.put("x-expires", ttlConfig.getTtl().getSeconds() * 1000);
        }
        return ttlOpts;
    }

    public ConnectionShutdownRoutine connectionShutdownRoutine() {
        if (this.connectionShutdownRoutine != null) {
            return this.connectionShutdownRoutine;
        }

        this.connectionShutdownRoutine = new ConnectionShutdownRoutine();
        return this.connectionShutdownRoutine;
    }

    private class ConnectionShutdownRoutine implements Callable<Boolean> {

        @Override
        public Boolean call() {
            try {
                retryer.call(() -> {
                    final long totalConsumersWithActiveMessagesPerConnection = connectionMeta.getConsumerMeta().values().stream()
                            .map(consumerMetas -> consumerMetas.stream()
                                    .map(ConsumerMeta::getActiveMessagesCount)
                                    .filter(count -> count > 0)
                                    .count())
                            .reduce(0L, Long::sum);

                    if (totalConsumersWithActiveMessagesPerConnection > 0) {
                        log.info("Number of consumers with activeMessage: {} for connection: {}",
                                totalConsumersWithActiveMessagesPerConnection, name);

                        log.error("Throwing exception, will try closing connection {} again in sometime.", name);
                        throw RabbitmqActorException.builder()
                                .errorCode(ErrorCode.CONNECTION_SHUTDOWN_ERROR)
                                .message("Error in closing connection.")
                                .build();
                    }

                    if (null != channel && channel.isOpen()) {
                        channel.close();
                    }

                    if (null != connection && connection.isOpen()) {
                        log.info("Closing connection: {}", name);
                        connection.close();
                    }
                    return true;
                });
            } catch (Exception e) {
                log.error("Exception while closing connection: {}", name);
            }
            return true;
        }
    }
}
