package org.demo.batchdemo.scorejob.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StudentResult {
    private String name;
    private int avg;
    private int max;
}
