package com.asuscomm.jkh120.code01.batch.configuration;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.FileCopyUtils;

@Configuration
@EnableBatchProcessing
public class JobConfiguration {

	private static final String JOB_NAME = "dbLoaderJob";

	private static final int JOB_FILE_COUNT = 5;
	// private static final String JSON_PATH = "C:/Users/Jeong/Desktop/";
	private static final String JSON_PATH = "/root/dev/java/batch/code01/";

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private Step dbLoaderStep;

	@Autowired
	private SimpleJobLauncher jobLauncher;

	@Autowired
	private MultiResourceItemReader itemReader;

	@Bean
	public Job job() {
		return jobBuilderFactory.get(JOB_NAME).incrementer(new RunIdIncrementer()).listener(executeListener())
				.start(dbLoaderStep).build();
	}

//	@Scheduled(fixedRate = 10000)
	@Scheduled(cron = "0 0 * * * *")
	public void runJob() throws Exception {
		System.out.println("Job Started at :" + new Date());

		JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();

		JobExecution execution = jobLauncher.run(job(), param);

		System.out.println("Job finished with status :" + execution.getStatus());
	}

	@Autowired
	private ApplicationContext applicationContext;

	@Bean
	public JobExecutionListener executeListener() {
		return new JobExecutionListener() {

			private Resource[] resources;
			private String path = JSON_PATH;

			@Override
			public void beforeJob(JobExecution jobExecution) {
				System.out.println("pre process");

				try {

					Resource[] tmpResources = applicationContext.getResources("file:" + path + "*.json");

					int fileLength = JOB_FILE_COUNT;

					if (tmpResources.length < fileLength) {
						resources = new Resource[tmpResources.length];
					} else {
						resources = new Resource[fileLength];
					}

					for (int i = 0; i < resources.length; i++) {
						resources[i] = tmpResources[i];
					}

					itemReader.setResources(resources);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

			@Override
			public void afterJob(JobExecution jobExecution) {
				// TODO Auto-generated method stub
				for (Resource resource : resources) {
					try {
						File oldFile = resource.getFile();
						File newFile = new File(path + "done/" + resource.getFilename());
						FileCopyUtils.copy(oldFile, newFile);
						oldFile.delete();

					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}

		};
	}

}
