package com.asuscomm.jkh120.code01.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class BatchUploadForDaumApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchUploadForDaumApplication.class, args);
	}
}
