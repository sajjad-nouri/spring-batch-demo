package org.demo.batchdemo.scorejob;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ScoreJobConfiguration {

    @Bean
    public Job scoreJob(JobRepository jobRepository, Step averageStep, Step bestStep) {
        return new JobBuilder("score-job", jobRepository)
                .start(averageStep)
                .next(bestStep)
                .build();
    }
}
