package org.demo.batchdemo.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/batch")
@RequiredArgsConstructor
public class JobController {
    private final JobLauncher jobLauncher;
    private final Job scoreJob;
    private final Job helloJob;

    @GetMapping("/hello-job/run")
    public String runHelloJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                        .toJobParameters();

        jobLauncher.run(helloJob, jobParameters);
        return "hello job triggered";
    }

    @GetMapping("/score-job/run")
    public String runScoreJob(@RequestParam String fileInput, @RequestParam String fileOutput) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("fileInput", fileInput, true)
                .addString("fileOutput", fileOutput, false)
                        .toJobParameters();

        jobLauncher.run(scoreJob, jobParameters);
        return String.format("score job triggered. fileInput: %s, fileOutput: %s ", fileInput, fileOutput);
    }
}
