package com.example.demo.remote.chunk;

import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@JobScope
@Component
public class JobContext {

    private final Integer jobId;

    public JobContext(@Value("#{jobParameters['jobId']}") Integer jobId) {
        this.jobId = jobId;
    }

    public Integer getJobId() {
        return jobId;
    }

}
