
package com.auth.encryptutil.configuration;

import javax.sql.DataSource;

import com.auth.encryptutil.util.SecretKeyGeneration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
public class JDBCConfiguration {

    @Autowired
    SystemProperties properties;
    @Bean
    public DataSource dataSource() {

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(properties.getDriverName());
        dataSource.setUrl(properties.getJdbcUrl());
        dataSource.setUsername(properties.getDatabaseUserName());
        dataSource.setPassword(SecretKeyGeneration.decrypt(properties.getDatabasePassword()));
        return dataSource;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(dataSource());
        return jdbcTemplate;
    }
}
 
