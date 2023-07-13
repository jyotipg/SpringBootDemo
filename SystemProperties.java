package com.auth.encryptutil.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;

@Configuration
@ConfigurationProperties("app")
public class SystemProperties implements Serializable {

    private String jdbcUrl;

    private String driverName;
    private String databaseUserName;

    private String databasePassword;

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public void setDatabaseUserName(String databaseUserName) {
        this.databaseUserName = databaseUserName;
    }

    public void setDatabasePassword(String databasePassword) {
        this.databasePassword = databasePassword;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getDatabaseUserName() {
        return databaseUserName;
    }

    public String getDatabasePassword() {
        return databasePassword;
    }

    public String getDriverName() {
        return driverName;
    }
}
