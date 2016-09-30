package org.analyzr.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.datasource.embedded.ConnectionProperties;
import org.springframework.jdbc.datasource.embedded.DataSourceFactory;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Driver;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * @author nhuda
 * @since 29/09/16
 */
@Configuration
@ComponentScan("org.analyzr.service")
public class DaoConfig {
    @Primary
    @Bean
    public DataSource dataSourceJob() {
        return getEmbeddedDatabaseBuilder()
//                .addScript("classpath:sql/schema.sql")
                .build();
    }

    private EmbeddedDatabaseBuilder getEmbeddedDatabaseBuilder() {
        EmbeddedDatabaseBuilder databaseBuilder = new EmbeddedDatabaseBuilder();
        databaseBuilder.setDataSourceFactory(new DataSourceFactory() {
            private final SimpleDriverDataSource dataSource = new SimpleDriverDataSource();

            @Override
            public ConnectionProperties getConnectionProperties() {
                return new ConnectionProperties() {
                    @Override
                    public void setDriverClass(Class<? extends Driver> driverClass) {
                        dataSource.setDriverClass(org.hsqldb.jdbcDriver.class);
                    }

                    @Override
                    public void setUrl(String url) {
                        dataSource.setUrl("jdbc:hsqldb:mem:testdb;" +
                                "sql.syntax_mys=true;sql.enforce_strict_size=true;hsqldb.tx=mvcc");
                    }

                    @Override
                    public void setUsername(String username) {
                        dataSource.setUsername("sa");
                    }

                    @Override
                    public void setPassword(String password) {
                        dataSource.setPassword("");
                    }
                };
            }
            @Override
            public DataSource getDataSource() {
                return dataSource;
            }
        });

        return databaseBuilder;
    }


    @Autowired
    private DataSource dataSource;

    @Bean
    public PlatformTransactionManager transactionManagerUserContent() {
        DataSourceTransactionManager txManager = new DataSourceTransactionManager();
        txManager.setDataSource(dataSource);
        return txManager;
    }

}