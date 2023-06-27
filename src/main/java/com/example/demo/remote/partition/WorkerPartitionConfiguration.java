package com.example.demo.remote.partition;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing(tablePrefix = "BATCH2_")
@EnableBatchIntegration
@Profile("worker-partition")
public class WorkerPartitionConfiguration {

    private final RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory;

    private final KafkaTemplate<String, StepExecutionRequest> kafkaTemplate;

    public WorkerPartitionConfiguration(RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory,
            KafkaTemplate<String, StepExecutionRequest> kafkaTemplate) {
        super();
        this.workerStepBuilderFactory = workerStepBuilderFactory;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    public QueueChannel inboundChannel() {
        return new QueueChannel();
    }

    @Bean
    public IntegrationFlow intboundFlow(ConsumerFactory<String, StepExecutionRequest> consumerFactory) {
        return IntegrationFlow.from(Kafka.messageDrivenChannelAdapter(consumerFactory, "partition-requests"))
                .log(LoggingHandler.Level.INFO).channel(inboundChannel()).get();
    }

    /*
     * Configure outbound flow (replies going to the manager)
     */
    @Bean
    public DirectChannel outboundChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow() {
        KafkaProducerMessageHandler<String, StepExecutionRequest> kafkaProducerMessageHandler = new KafkaProducerMessageHandler<>(
                kafkaTemplate);
        kafkaProducerMessageHandler.setTopicExpression(new LiteralExpression("patition-replies"));
        return IntegrationFlow.from(outboundChannel()).log(LoggingHandler.Level.INFO)
                .handle(kafkaProducerMessageHandler).get();

    }

    /*
     * Configure the worker step
     */
    @Bean
    public Step workerStep(PlatformTransactionManager transactionManager) {
        return this.workerStepBuilderFactory.get("workerStep")
                .inputChannel(inboundChannel())
                // .outputChannel(outboundChannel())
                .tasklet(tasklet(null, null, null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{stepExecutionContext['partition_number']}") String partition,
            @Value("#{stepExecutionContext['first_line']}") Long firstLine,
            @Value("#{stepExecutionContext['last_line']}") Long lastLine) {
        return (contribution, chunkContext) -> {

            // if ("0".equals(partition)) {
            //     throw new RuntimeException("Erro no partition_number_0");
            // }

            System.out.println("partition_number " + partition);
            System.out.println("first_line " + firstLine);
            System.out.println("last_line " + lastLine);
            return RepeatStatus.FINISHED;
        };
    }

}
