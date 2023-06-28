package com.example.demo.remote.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
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

import com.example.demo.remote.partition.constants.BatchConstants;
import com.example.demo.remote.partition.model.Student;

@Configuration
@EnableBatchProcessing(tablePrefix = "BATCH2_")
@EnableBatchIntegration
@Profile("worker-partition")
public class WorkerPartitionConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(WorkerPartitionConfiguration.class);

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
        return workerStepBuilderFactory.get("workerStep")
                .inputChannel(inboundChannel())
                .chunk(100, transactionManager)
                .reader(flatFileItemReader(null, null, null))
                // .processor(itemProcessor())
                .writer(items -> logger.info("Received chunk size: {}", items.size()))
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Student> flatFileItemReader(
            @Value("#{stepExecutionContext[partition_number]}") final Long partitionNumber,
            @Value("#{stepExecutionContext[first_line]}") final Long firstLine,
            @Value("#{stepExecutionContext[item_count]}") final Long item_count) {

        logger.info("Partition Number : {}, Reading file from line : {}, to line: {} ", partitionNumber, firstLine,
                item_count);

        // if(partitionNumber == 1) {
        //     throw new RuntimeException("Partition Number : " + partitionNumber + " is not allowed");
        // }

        return new FlatFileItemReaderBuilder<Student>()
                .name("flatFileItemReader")
                .resource(new ClassPathResource(BatchConstants.STUDENTS_FILENAME))
                .delimited()
                .names("FirstName", "LastName", "Email", "Gender")
                .targetType(Student.class)
                .linesToSkip(firstLine.intValue())
                .maxItemCount(item_count.intValue())
                .build();

    }

}
