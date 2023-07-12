package com.example.demo.remote.partition.controller;

import java.util.UUID;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("students")
@Profile("manager-partition")
public class StudentJobController {

    private final Job job;

    private final JobLauncher jobLauncher;

    public StudentJobController(Job job, JobLauncher jobLauncher) {
        super();
        this.job = job;
        this.jobLauncher = jobLauncher;
    }

    @GetMapping
    public ResponseEntity<String> loadStudents() throws Exception {

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("nomeArquivo", UUID.randomUUID().toString())
                .toJobParameters();

        JobExecution jobExecution = jobLauncher.run(job, jobParameters);

        return ResponseEntity.ok("Job with Id : " + jobExecution.getJobId() + ", Successfully Started With Status : "
                + jobExecution.getStatus().name());
    }
}
