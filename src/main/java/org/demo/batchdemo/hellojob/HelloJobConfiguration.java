package org.demo.batchdemo.hellojob;

import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Arrays;

@Configuration
public class HelloJobConfiguration {
    @Bean
    public JobExecutionDecider decider() {
        return (jobExecution, stepExecution) -> new FlowExecutionStatus("first");
    }

    @Bean
    public Job helloJob(JobRepository jobRepository, Step step0, Step step1, Step step2, Step step3, Step step4) {
//        Flow deciderFlow = new FlowBuilder<Flow>("firstFLow")
//                .start(decider())
//                .on("first").to(step1)
//                .on("second").to(step2)
//                .build();

        Flow flow1 = new FlowBuilder<Flow>("flow1")
                .start(step1)
                .build();
        Flow flow2 = new FlowBuilder<Flow>("flow2")
                .start(step2)
                .build();
        Flow splitFlow = new FlowBuilder<Flow>("splitFlow")
                .split(new SimpleAsyncTaskExecutor())
                .add(flow1, flow2)
                .end();
        Flow masterFlow = new FlowBuilder<Flow>("masterFlow")
                .start(step0)
                .next(splitFlow)
                .build();

        return new JobBuilder("helloJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(masterFlow)
                .next(step3)
                .end()
                .build();
    }

    @Bean
    public Step step0(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step0", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    JobExecution jobExecution = contribution.getStepExecution().getJobExecution();

                    Thread.sleep(3000);
                    System.out.println("Step 0 tasklet executed." );
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step1", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    // Access JobExecutionContext
                    JobExecution jobExecution = contribution.getStepExecution().getJobExecution();
                    jobExecution.getExecutionContext().put("jobKey", "valueStoredInJobExecutionContext");

                    // Access StepExecutionContext
                    StepExecution stepExecution = contribution.getStepExecution();
                    stepExecution.getExecutionContext().put("stepKey", "valueStoredInStepExecutionContext");

                    contribution.setExitStatus(new ExitStatus("WAY2"));

                    Thread.sleep(3000);
                    System.out.println("Step 1 tasklet executed");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step2", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    JobExecution jobExecution = contribution.getStepExecution().getJobExecution();

                    Thread.sleep(3000);
                    System.out.println("Step 2 tasklet executed." );
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step3(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step3", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    JobExecution jobExecution = contribution.getStepExecution().getJobExecution();
//                    contribution.setExitStatus(new ExitStatus("WAY2"));

                    String jobKey = jobExecution.getExecutionContext().getString("jobKey");
                    Thread.sleep(3000);
                    System.out.println("Step 3 tasklet executed. jobKey: " + jobKey);
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step step4(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step4", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    JobExecution jobExecution = contribution.getStepExecution().getJobExecution();

                    String jobKey = jobExecution.getExecutionContext().getString("jobKey");
                    Thread.sleep(3000);
                    System.out.println("Step 4 tasklet executed. jobKey: " + jobKey);
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

//    @Bean
//    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws InterruptedException {
//        return new StepBuilder("step2", jobRepository)
//                .<String, String> chunk(10, transactionManager)
//                .reader(reader())
//                .processor(processor())
//                .writer(writer())
//                .build();
//    }
//
//    @Bean
//    public ItemReader<String> reader() {
//        return new ListItemReader<>(Arrays.asList("first", "second", "third"));
//    }
//
//    @Bean
//    public ItemProcessor<String, String> processor() throws InterruptedException {
//        Thread.sleep(3000);
//        return String::toUpperCase;
//    }
//
//    @Bean
//    public ItemWriter<String> writer() {
//        return items -> {
//            for (final String item : items) {
//                System.out.println("Writing " + item);
//            }
//        };
//    }
}