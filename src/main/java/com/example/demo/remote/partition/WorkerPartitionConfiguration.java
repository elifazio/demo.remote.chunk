package com.example.demo.remote.partition;

import org.apache.sshd.sftp.client.SftpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.file.remote.RemoteFileTemplate;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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

    private static final Integer THREAD_POOL_SIZE = 5;

    public WorkerPartitionConfiguration(final RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory,
            final KafkaTemplate<String, StepExecutionRequest> kafkaTemplate) {
        super();
        this.workerStepBuilderFactory = workerStepBuilderFactory;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    public QueueChannel inboundChannel() {
        return new QueueChannel();
    }

    @Bean
    public IntegrationFlow intboundFlow(final ConsumerFactory<String, StepExecutionRequest> consumerFactory) {
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
        final KafkaProducerMessageHandler<String, StepExecutionRequest> kafkaProducerMessageHandler = new KafkaProducerMessageHandler<>(
                kafkaTemplate);
        kafkaProducerMessageHandler.setTopicExpression(new LiteralExpression("patition-replies"));
        return IntegrationFlow.from(outboundChannel()).log(LoggingHandler.Level.INFO)
                .handle(kafkaProducerMessageHandler).get();

    }

    /*
     * Configure the worker step
     */
    @Bean
    public Step workerStep(final PlatformTransactionManager transactionManager, final ItemReader<Student> itemReader) {
        return workerStepBuilderFactory.get("workerStep")
                .inputChannel(inboundChannel())
                .<Student, Student>chunk(100, transactionManager)
                .reader(itemReader)
                .processor(item -> {
                    logger.info("Processing item: {}", item);
                    return item;
                })
                .writer(items -> logger.info("Received chunk size: {}", items.size()))
                .taskExecutor(this.taskExecutor())
                .build();
    }

    @Bean
    @StepScope
    public SynchronizedItemStreamReader<Student> itemReader(
            @Value("#{stepExecutionContext[partition_number]}") final Long partitionNumber,
            @Value("#{stepExecutionContext[first_line]}") final Long firstLine,
            @Value("#{stepExecutionContext[item_count]}") final Long item_count,
            final RemoteFileTemplate<SftpClient.DirEntry> remoteFileTemplate) {

        logger.info("Partition Number : {}, Reading file from line : {}, to line: {} ", partitionNumber, firstLine,
                item_count);

        final String remoteFilePath = BatchConstants.REMOTO_FILE_PATH + BatchConstants.FILENAME;

        final FlatFileItemReader<Student> flatFileItemReader = new FlatFileItemReaderBuilder<Student>()
                .name("flatFileItemReader")
                .resource(new SftpInputStreamResource(remoteFileTemplate, remoteFilePath))
                .delimited()
                .names("FirstName", "LastName", "Email", "Gender")
                .targetType(Student.class)
                .saveState(false)
                .linesToSkip(firstLine.intValue())
                .maxItemCount(item_count.intValue())
                .build();

        final SynchronizedItemStreamReader<Student> synchronizedItemStreamReader = new SynchronizedItemStreamReader<>();
        synchronizedItemStreamReader.setDelegate(flatFileItemReader);
        return synchronizedItemStreamReader;
    }

    private ThreadPoolTaskExecutor taskExecutor() {
        final int numeroMinimoThreads = 3;
        final ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(THREAD_POOL_SIZE);
        taskExecutor.setCorePoolSize(THREAD_POOL_SIZE < numeroMinimoThreads ? numeroMinimoThreads : THREAD_POOL_SIZE);
        taskExecutor.setQueueCapacity(THREAD_POOL_SIZE);
        taskExecutor.setThreadNamePrefix("WorkerStep-Thread-");
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

}
