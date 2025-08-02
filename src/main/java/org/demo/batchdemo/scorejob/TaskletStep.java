package org.demo.batchdemo.scorejob;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.demo.batchdemo.scorejob.model.StudentResult;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class TaskletStep {

    @Bean
    public Step bestStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("select-best-student", jobRepository)
                .tasklet(bestTasklet(), platformTransactionManager)
                .build();
    }

    @Bean
    public Tasklet bestTasklet() {
        return (contribution, chunkContext) -> {
            String fileOutput = chunkContext.getStepContext().getJobParameters().get("fileOutput").toString();
            Path path = Paths.get(fileOutput.substring(5));
            try(Stream<String> line = Files.lines(path)) {
                List<StudentResult> studentResultList = line.map(this::parseLine).toList();

                StepExecution stepExecution = contribution.getStepExecution();
                if(!stepExecution.getExecutionContext().containsKey("calc-step"))
                    stepExecution.getExecutionContext().put("calc-step", "best");
                else if(stepExecution.getExecutionContext().get("calc-step") == "best") {
                    Optional<StudentResult> bestStudent = studentResultList.stream().max(Comparator.comparingInt(StudentResult::getAvg));
                    System.out.println("$$$ BEST STUDENT IS : " + bestStudent.get().getName() + " WITH AVG " + bestStudent.get().getAvg() + " :)");
                    stepExecution.getExecutionContext().put("calc-step", "worst");
                } else if(stepExecution.getExecutionContext().get("calc-step") == "worst") {
                    Optional<StudentResult> bestStudent = studentResultList.stream().min(Comparator.comparingInt(StudentResult::getAvg));
                    System.out.println("$$$ WORST STUDENT IS : " + bestStudent.get().getName() + " WITH AVG " + bestStudent.get().getAvg() + " :)");
                    stepExecution.getExecutionContext().put("calc-step", "finished");
                    return RepeatStatus.FINISHED;
                }
            }
            return RepeatStatus.CONTINUABLE;
        };
    }

    private StudentResult parseLine(String line) {
        String[] split = line.split(",");
        return new StudentResult(split[0], Integer.parseInt(split[1]), Integer.parseInt(split[2]));
    }

}
