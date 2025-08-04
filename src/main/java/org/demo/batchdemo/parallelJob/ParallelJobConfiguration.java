package org.demo.batchdemo.parallelJob;

import org.apache.logging.log4j.util.Strings;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.task.ThreadPoolTaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Arrays;

@Configuration
public class ParallelJobConfiguration {
    private TaskExecutor createTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutorBuilder()
                .corePoolSize(2)
                .maxPoolSize(15)
                .queueCapacity(100)
                .threadNamePrefix("parallel-task-executor-")
                .build();
        taskExecutor.initialize();
        return taskExecutor;
    }

    @Bean
    public Job parallelJob(JobRepository jobRepository, Step stepUppercase) {
        return new JobBuilder("parallelJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(stepUppercase)
                .build();
    }

    @Bean
    public Step stepUppercase(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws InterruptedException {
        return new StepBuilder("step2", jobRepository)
                .<String, String>chunk(1, transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .taskExecutor(createTaskExecutor())
                .listener(new ItemReadListener<String>() {

                    @Override
                    public void afterRead(String item) {
                        System.out.println("after read: " + item);
                    }
                })
                .build();
    }

    @Bean
    public ItemReader<String> reader() {
        return new ListItemReader<>(Arrays.asList(
                "x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x10",
                "x11", "x12", "x13", "x14", "x15", "x16", "x17", "x18", "x19", "x20"
        ));
    }

    @Bean
    public ItemProcessor<String, String> processor() throws InterruptedException {
        return value -> {
            Thread.sleep(2000);
            return Strings.toRootUpperCase(value);
        };
    }

    @Bean
    public ItemWriter<String> writer() {
        return items -> {
            System.out.println("Writing " + String.join(" ", items.getItems()));
        };
    }
}