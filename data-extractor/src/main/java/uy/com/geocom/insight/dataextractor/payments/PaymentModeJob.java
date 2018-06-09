package uy.com.geocom.insight.dataextractor.payments;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import uy.com.geocom.insight.dataextractor.InsightExtractorProperties;

import javax.sql.DataSource;
import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;

@Configuration
public class PaymentModeJob {

    private final InsightExtractorProperties properties;

    private final DataSource dataSource;

    private final StepBuilderFactory stepBuilderFactory;

    private final JobBuilderFactory jobBuilderFactory;

    public PaymentModeJob(InsightExtractorProperties properties, DataSource dataSource, StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.properties = properties;
        this.dataSource = dataSource;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    ItemReader<PaymentModeDTO> paymentModesDatabaseReader() {
        JdbcPagingItemReader<PaymentModeDTO> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setPageSize(100);
        reader.setRowMapper(new BeanPropertyRowMapper<>(PaymentModeDTO.class));
        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT pm.id, pm.name, pmt.name as type");
        queryProvider.setFromClause("from paymentmodes pm LEFT JOIN paymentmodetypes pmt ON pm.paymentmodetype = pmt.id");
        HashMap<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("pm.id", Order.DESCENDING);
        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);
        return reader;
    }

    @Bean
    ItemProcessor<PaymentModeDTO, PaymentModeDTO> paymentModeProcessor() {
        return item -> item;
    }

    @Bean
    ItemWriter<PaymentModeDTO> paymentsModeCVSWriter() {
        FlatFileItemWriter<PaymentModeDTO> writer = new FlatFileItemWriter<>();
        writer.setHeaderCallback(writer1 -> writer1.write("ID, NAME, TYPE"));
        writer.setResource(new FileSystemResource(properties.getPath() + File.separator + "payments-mode.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<>());

        return writer;
    }

    @Bean
    Step paymentModesDBToCsvFileStep() {
        return stepBuilderFactory.get("paymentModesDBToCsvFileStep")
                .<PaymentModeDTO, PaymentModeDTO>chunk(100)
                .reader(paymentModesDatabaseReader())
                .processor(paymentModeProcessor())
                .writer(paymentsModeCVSWriter())
                .build();
    }

    @Bean("payment-modes-db-csv-job")
    Job paymentModesDbToCsvFIleJob() {
        return jobBuilderFactory.get("paymentModesDbToCsvFIleJob")
                .incrementer(new RunIdIncrementer())
                .flow(paymentModesDBToCsvFileStep())
                .end()
                .build();
    }
}
