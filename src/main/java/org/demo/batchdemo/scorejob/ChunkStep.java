package org.demo.batchdemo.scorejob;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.demo.batchdemo.scorejob.model.Student;
import org.demo.batchdemo.scorejob.model.StudentResult;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.WritableResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.Future;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ChunkStep {

    @Bean
    public Step averageStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager,
                            ItemReader<Student> threadSafeReader,
                            AsyncItemProcessor<Student, StudentResult> asyncAverageProcessor,
                            AsyncItemWriter<StudentResult> asyncAverageWriter) {
        return new StepBuilder("averageStep", jobRepository)
                .<Student, Future<StudentResult>>chunk(3, platformTransactionManager)
                .reader(threadSafeReader)
                .processor(asyncAverageProcessor)
                .writer(asyncAverageWriter)
//                .taskExecutor(new SimpleAsyncTaskExecutor("score-job-task-executor"))
                .listener(new ItemReadListener<Student>() {
                    @Override
                    public void afterRead(Student item) {
                        System.out.println("average step after read: " + item);
                    }
                })
                .listener(new ItemWriteListener<StudentResult>() {
                    @Override
                    public void afterWrite(Chunk<? extends StudentResult> items) {
                        System.out.println("average step after write: " + items);
                    }
                }).listener(new ChunkListener() {
                    @Override
                    public void beforeChunk(ChunkContext context) {
                        System.out.println("average step beforeChunk: " + context);
                    }

                    @Override
                    public void afterChunk(ChunkContext context) {
                        System.out.println("average step afterChunk: " + context);
                    }
                })

                .build();
    }

    @Bean
    public ItemReader<Student> threadSafeReader(FlatFileItemReader<Student> averageStepReader) {
        SynchronizedItemStreamReader<Student> reader = new SynchronizedItemStreamReader<>();
        reader.setDelegate(averageStepReader);
        return reader;
    }

    @StepScope
    @Bean
    public FlatFileItemReader<Student> averageStepReader(@Value("#{jobParameters['fileInput']}") String inputFile) {
        return new FlatFileItemReaderBuilder<Student>()
                .name("averageStepReader")
                .resource(new ClassPathResource(inputFile))
                .delimited().delimiter(",")
                .names("name", "course1", "course2", "course3")
                .targetType(Student.class)
                .build();
    }

    @Bean
    public AsyncItemProcessor<Student, StudentResult> asyncAverageProcessor() {
        AsyncItemProcessor<Student, StudentResult> processor = new AsyncItemProcessor<>();
        processor.setDelegate(averageProcessor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return processor;
    }

    @Bean
    public ItemProcessor<Student, StudentResult> averageProcessor() {
        return student -> {
            System.out.println("average processor student: " + student + " thread name: " + Thread.currentThread().getName());
            Thread.sleep(3000);
            StudentResult studentResult = new StudentResult();
            studentResult.setName(student.getName());
            studentResult.setAvg((student.getCourse1() + student.getCourse2() + student.getCourse3()) / 3);
            studentResult.setMax(Math.max(Math.max(student.getCourse1(), student.getCourse2()), student.getCourse3()));
            return studentResult;
        };
    }

    @Bean
    @StepScope
    public AsyncItemWriter<StudentResult> asyncAverageWriter(@Value("#{jobParameters['fileOutput']}") WritableResource outputFile) {
        AsyncItemWriter<StudentResult> asyncWriter = new AsyncItemWriter<>();
        LoggingItemWriter<StudentResult> loggingItemWriter = new LoggingItemWriter<>(averageStepWriter(outputFile));
        asyncWriter.setDelegate(loggingItemWriter);
        return asyncWriter;
    }

    @StepScope
    @Bean
    public FlatFileItemWriter<StudentResult> averageStepWriter(@Value("#{jobParameters['fileOutput']}") WritableResource outputFile) {
        return new FlatFileItemWriterBuilder<StudentResult>()
                .name("averageStepWriter")
                .resource(outputFile)
                .formatted()
                .format("%s,%s,%s")
                .names("name", "avg", "max")
                .build();
    }

}
