//package org.demo.batchdemo;
//
//import lombok.RequiredArgsConstructor;
//import org.springframework.batch.core.*;
//import org.springframework.batch.core.launch.JobLauncher;
//import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
//import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
//import org.springframework.batch.core.repository.JobRestartException;
//import org.springframework.stereotype.Component;
//
//@RequiredArgsConstructor
//@Component
//public class Scheduler {
//    private final JobLauncher jobLauncher;
//    private final Job helloJob;
//
////    @Scheduled(fixedDelay = 2000, initialDelay = 1000)
//    public void scheduledRun() {
//        try {
//            jobLauncher.run(helloJob, new JobParametersBuilder()
//                    .addLong("time", System.currentTimeMillis())
//                    .toJobParameters());
//        } catch (JobExecutionAlreadyRunningException e) {
//            throw new RuntimeException(e);
//        } catch (JobRestartException e) {
//            throw new RuntimeException(e);
//        } catch (JobInstanceAlreadyCompleteException e) {
//            throw new RuntimeException(e);
//        } catch (JobParametersInvalidException e) {
//            throw new RuntimeException(e);
//        }
//    }
//}
