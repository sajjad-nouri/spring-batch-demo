//package org.demo.batchdemo;
//
//import lombok.RequiredArgsConstructor;
//import org.springframework.batch.core.Job;
//import org.springframework.batch.core.JobParameters;
//import org.springframework.batch.core.JobParametersBuilder;
//import org.springframework.batch.core.launch.JobLauncher;
//import org.springframework.boot.CommandLineRunner;
//import org.springframework.stereotype.Component;
//
//@Component
//@RequiredArgsConstructor
//public class JobStarter implements CommandLineRunner {
//
//    private final JobLauncher jobLauncher;
//    private final Job helloJob;
//
//    @Override
//    public void run(String... args) throws Exception {
//        JobParameters params = new JobParametersBuilder()
////                .addLong("run.id", System.currentTimeMillis()) // needed if you use RunIdIncrementer
//                .addString("name", args[0])
//                .toJobParameters();
//
//        jobLauncher.run(helloJob, params);
//    }
//}
