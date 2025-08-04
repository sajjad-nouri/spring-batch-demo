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
import org.springframework.boot.task.ThreadPoolTaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Arrays;

@Configuration
public class HelloJobConfiguration {
    private TaskExecutor createTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutorBuilder()
                .corePoolSize(2)
                .maxPoolSize(2)
                .queueCapacity(100)
                .threadNamePrefix("split-executor")
                .build();
        taskExecutor.initialize();
        return taskExecutor;
    }

    @Bean
    public Job helloJob(JobRepository jobRepository, Step step0,Step step1, Step step2, Step step3, Step step4) {
        Flow splitFlow = new FlowBuilder<Flow>("split-flow")
                .split(createTaskExecutor())
                .add(
                        new FlowBuilder<Flow>("flow1").start(step1).build(),
                        new FlowBuilder<Flow>("flow2").start(step2).build()
                )
                .build();

        Flow flow0 = new FlowBuilder<Flow>("split-0")
                .start(step0)
                .build();

        Flow conditionalFlow = new FlowBuilder<Flow>("split-0")
                .start(step0)
                .on("YES").to(step1)
                .from(step0).on("NO").to(step2)
                .build();

        return new JobBuilder("helloJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(flow0)
                .next(splitFlow)
                .next(step4)
                .end()
                .build();
    }

    @Bean
    public Step step0(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step0", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(3000);
                    System.out.println("Step 0 tasklet executed." );
//                    contribution.setExitStatus(new ExitStatus("NO"));
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step1", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(3000);
                    System.out.println("Step 1 tasklet executed");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step2", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(3000);
                    System.out.println("Step 2 tasklet executed");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Step step3(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step3", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(3000);
                    System.out.println("Step 3 tasklet executed");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Step step4(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step4", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(3000);
                    System.out.println("Step 4 tasklet executed");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .allowStartIfComplete(true)
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