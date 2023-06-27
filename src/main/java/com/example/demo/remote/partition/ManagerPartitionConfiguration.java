package com.example.demo.remote.partition;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
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
import com.example.demo.remote.partition.constants.BatchConstants;
import com.example.demo.remote.partition.partitioner.CsvStepPartitioner;

/**
 * This configuration class is for the manager side of the remote partitioning
 * sample. The
 * manager step will create 3 partitions for workers to process.
 *
 * @author Mahmoud Ben Hassine
 */
@Configuration
@EnableBatchProcessing(tablePrefix = "BATCH2_")
@EnableBatchIntegration
@Profile("manager-partition")
public class ManagerPartitionConfiguration {

    private final RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory;

    private final KafkaTemplate<String, StepExecutionRequest> kafkaTemplate;

    public ManagerPartitionConfiguration(RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory,
            KafkaTemplate<String, StepExecutionRequest> kafkaTemplate) {
        this.managerStepBuilderFactory = managerStepBuilderFactory;
        this.kafkaTemplate = kafkaTemplate;
    }

    /*
     * Configure the manager step
     */
    @Bean
    public Step managerStep() {
        return this.managerStepBuilderFactory.get(BatchConstants.LOAD_CSV_STEP_PARTITIONER).partitioner("workerStep", new CsvStepPartitioner())
                .gridSize(BatchConstants.GRID_SIZE)
                .outputChannel(outboundChannel())
                // .inputChannel(inboundChannel())
                .build();
    }

    @Bean
    public Job remotePartitioningJob(JobRepository jobRepository) {
        return new JobBuilder(BatchConstants.LOAD_CSV_FILE_JOB, jobRepository).start(managerStep()).build();
    }

    // envia msg para o worker
    @Bean
    public DirectChannel outboundChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow() {
        KafkaProducerMessageHandler<String, StepExecutionRequest> kafkaProducerMessageHandler = new KafkaProducerMessageHandler<>(
                kafkaTemplate);
        kafkaProducerMessageHandler.setTopicExpression(new LiteralExpression("partition-requests"));
        return IntegrationFlow.from(outboundChannel()).log(LoggingHandler.Level.INFO)
                .handle(kafkaProducerMessageHandler).get();

    }

    // recebe msg worker
    @Bean
    public QueueChannel inboundChannel() {
        return new QueueChannel();
    }

    @Bean
    public IntegrationFlow intboundFlow(ConsumerFactory<String, StepExecutionRequest> consumerFactory,
            JobRepository jobRepository) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(consumerFactory, "patition-replies"))
                .log(LoggingHandler.Level.INFO)
                .channel(inboundChannel())
                .get();
    }
    
}
