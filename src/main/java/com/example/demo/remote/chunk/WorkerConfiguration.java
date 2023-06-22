/*
 * Copyright 2018-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.demo.remote.chunk;

import java.math.BigDecimal;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
@EnableIntegration
@Profile("worker")
public class WorkerConfiguration {

	private final RemoteChunkingWorkerBuilder<TransacaoDTO, TransacaoDTO> remoteChunkingWorkerBuilder;

	private final KafkaTemplate<String, TransacaoDTO> kafkaTemplate;

	public WorkerConfiguration(RemoteChunkingWorkerBuilder<TransacaoDTO, TransacaoDTO> remoteChunkingWorkerBuilder,
			KafkaTemplate<String, TransacaoDTO> kafkaTemplate) {
		super();
		this.remoteChunkingWorkerBuilder = remoteChunkingWorkerBuilder;
		this.kafkaTemplate = kafkaTemplate;
	}

	@Bean
	public IntegrationFlow stepWorker() {
		return this.remoteChunkingWorkerBuilder.inputChannel(inboundChannel()).outputChannel(outboundChannel())
				.itemProcessor(transacao -> {
					if (transacao.getValor().compareTo(BigDecimal.ZERO) < 0) {
						//System.out.println(transacao.getValor());
					}
					if (transacao.getCartao().equals("3580601485757139")) {
						throw new RuntimeException("Cartão inválido");
					}
					throw new RuntimeException("Cartão inválido");
					//return transacao;
				}).itemWriter(items -> {
					System.out.println(items.size());
				})				
				.build();

	}

	@Bean
	public QueueChannel inboundChannel() {
		return new QueueChannel();
	}

	@Bean
	public IntegrationFlow intboundFlow(ConsumerFactory<String, TransacaoDTO> consumerFactory) {
		return IntegrationFlow.from(Kafka.messageDrivenChannelAdapter(consumerFactory, "chunk-requests"))
				.log(LoggingHandler.Level.INFO).channel(inboundChannel()).get();
	}

	@Bean
	public DirectChannel outboundChannel() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow outboundFlow() {
		KafkaProducerMessageHandler<String, TransacaoDTO> kafkaProducerMessageHandler = new KafkaProducerMessageHandler<String, TransacaoDTO>(
				kafkaTemplate);
		kafkaProducerMessageHandler.setTopicExpression(new LiteralExpression("chunk-replies"));
		return IntegrationFlow.from(outboundChannel()).log(LoggingHandler.Level.INFO)
				.handle(kafkaProducerMessageHandler).get();

	}

}
