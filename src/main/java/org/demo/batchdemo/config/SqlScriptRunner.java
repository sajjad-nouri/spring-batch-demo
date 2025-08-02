package org.demo.batchdemo.config;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class SqlScriptRunner {

    @Autowired
    private DataSource dataSource;

    @PostConstruct
    public void runSqlScript() {
        ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator(false, false, "UTF-8",
                new ClassPathResource("schema-h2.sql"));
        resourceDatabasePopulator.execute(dataSource);
    }
}
