package com.example.demo.remote.chunk;

import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.integration.chunk.ChunkResponse;
import org.springframework.integration.core.GenericSelector;
import org.springframework.stereotype.Component;

@Component
public class ChunkResponseFilter implements GenericSelector<ChunkResponse> {

    private JobExecution currentJobExecution;

    public void register(JobExecution jobExecution) {
        this.currentJobExecution = jobExecution;
    }

    public void unregister() {
        this.currentJobExecution = null;
    }

    @Override
    public boolean accept(ChunkResponse source) {
        LoggerFactory.getLogger(ChunkResponseFilter.class).info("{}",
                source);

        if (this.currentJobExecution != null) {
            // JobInstance jobInstance =
            // this.jobRepository.getJobInstance("job-manager-cabr", currentJobParameters);
            // LoggerFactory.getLogger(ChunckResponseFilter.class).info("{}", jobInstance);
            return source.getJobId().equals(this.currentJobExecution.getJobId());
        }

        return true;
    }

}
