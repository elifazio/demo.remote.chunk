package com.example.demo.remote.chunk;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobInstanceException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/processar")
@Profile("manager")
public class Controller {

	private final Job job;

	private final JobLauncher jobLauncher;
	private final JobOperator jobOperator;
	private final JobExplorer jobExplorer;

	public Controller(Job job, JobLauncher jobLauncher, ChunkResponseFilter chunkResponseFilter,
			JobOperator jobOperator, JobExplorer jobExplorer) {
		super();
		this.job = job;
		this.jobLauncher = jobLauncher;
		this.jobOperator = jobOperator;
		this.jobExplorer = jobExplorer;
	}

	@PostMapping
	public void processar() throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException, NoSuchJobInstanceException {

		JobParameters jobParameters = new JobParametersBuilder()
				.addString("filename", "transactions.csv")
				.toJobParameters();
/* 
		List<Long> jobInstances = Collections.emptyList()	;
		try {
			jobInstances = jobOperator.getJobInstances("job-manager-cabr", 0, 10);
		} catch (NoSuchJobException e) {
			LoggerFactory.getLogger(Controller.class)
					.error("Erro ao localizar o job job-manager-cabr a new job instance will be created. Cause: "
							+ e.getLocalizedMessage());
		}

		JobExecution run = null;
		if (!jobInstances.isEmpty()) {
			Long instanceId = jobInstances.get(0);
			List<Long> executions = jobOperator.getExecutions(instanceId);
			if (!executions.isEmpty()) {
				Long executionId = executions.get(0);
				JobExecution jobExecution = jobExplorer.getJobExecution(executionId);
				if (jobExecution.getStatus().equals(BatchStatus.FAILED)) {
					try {
						Long restartId = jobOperator.restart(executionId);
						run = jobExplorer.getJobExecution(restartId);
					} catch (Exception e) {
						LoggerFactory.getLogger(Controller.class)
								.error("Error resuming job " + executionId
										+ ", a new job instance will be created. Cause: "
										+ e.getLocalizedMessage());
					}
				}
			}
		}

		if (run == null) {
			run = jobLauncher.run(job, jobParameters);
		}*/

		JobExecution jobExecution = jobLauncher.run(job, jobParameters);
		BatchStatus batchStatus = jobExecution.getStatus();
	}

}
