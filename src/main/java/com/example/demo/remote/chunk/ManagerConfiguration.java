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

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.integration.chunk.ChunkResponse;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;

@Configuration
@EnableBatchProcessing(tablePrefix = "BATCH2_")
@EnableBatchIntegration
@EnableIntegration
@Profile("manager")
public class ManagerConfiguration {

	private final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory;

	private final KafkaTemplate<String, TransacaoDTO> kafkaTemplate;

	private String jobName;

	public ManagerConfiguration(RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory,
			KafkaTemplate<String, TransacaoDTO> kafkaTemplate) {
		super();
		this.remoteChunkingManagerStepBuilderFactory = remoteChunkingManagerStepBuilderFactory;
		this.kafkaTemplate = kafkaTemplate;
		this.jobName = "job-manager-cabr";
	}

	@Bean
	public Job jobManager(JobRepository jobRepository, Step stepManager, ChunkResponseFilter filter,
			JobRegistry jobRegistry) {
		Job job = new JobBuilder(jobName, jobRepository)
				.start(stepManager)
				.listener(new JobExecutionListener() {
					@Override
					public void beforeJob(org.springframework.batch.core.JobExecution jobExecution) {
						filter.register(jobExecution);
					}

					@Override
					public void afterJob(org.springframework.batch.core.JobExecution jobExecution) {
						filter.unregister();
					}
				})
				.build();

		return job;
	}

	@Bean
	public Step taskletStep(MessageChannel outboundChannel, PollableChannel inboundChannel) {
		MessagingTemplate messagingTemplate = new MessagingTemplate(outboundChannel);
		messagingTemplate.setReceiveTimeout(5000);

		return this.remoteChunkingManagerStepBuilderFactory.get("step-manager")
				.chunk(10)
				.reader(managerReader())
				.messagingTemplate(messagingTemplate)
				.throttleLimit(3L)
				.maxWaitTimeouts(5)
				.inputChannel(inboundChannel)
				.listener(new ChunkListener(){

					@Override
					public void afterChunkError(ChunkContext context) {
						ChunkListener.super.afterChunkError(context);
					}
					
				})

				.allowStartIfComplete(Boolean.TRUE)
				.build();
	}

	@Bean
	public FlatFileItemReader<TransacaoDTO> managerReader() {
		return new FlatFileItemReaderBuilder<TransacaoDTO>().resource(new ClassPathResource("/data/transactions.csv"))
				.name("manager-reader").delimited().delimiter(",").names("cartao", "valor", "data")
				.targetType(TransacaoDTO.class)
				.build();

	}

	// envia msg para o worker
	@Bean
	public DirectChannel outboundChannel() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow outboundFlow(MessageChannel outboundChannel) {
		KafkaProducerMessageHandler<String, TransacaoDTO> kafkaProducerMessageHandler = new KafkaProducerMessageHandler<String, TransacaoDTO>(
				kafkaTemplate);
		kafkaProducerMessageHandler.setTopicExpression(new LiteralExpression("chunk-requests"));
		return IntegrationFlow.from(outboundChannel).log(LoggingHandler.Level.INFO)
				.handle(kafkaProducerMessageHandler).get();

	}

	// recebe msg worker
	@Bean
	public QueueChannel inboundChannel() {
		return new QueueChannel();
	}

	@Bean
	public IntegrationFlow intboundFlow(ConsumerFactory<String, ChunkResponse> consumerFactory,
			JobRepository jobRepository, PollableChannel inboundChannel, ChunkResponseFilter filter) {
		return IntegrationFlow
				.from(Kafka.messageDrivenChannelAdapter(consumerFactory, "chunk-replies").filterInRetry(true))
				.log(LoggingHandler.Level.INFO)
				.filter(filter, arg0 -> arg0.discardChannel(new NullChannel()))
				.channel(inboundChannel)
				.get();
	}

}
