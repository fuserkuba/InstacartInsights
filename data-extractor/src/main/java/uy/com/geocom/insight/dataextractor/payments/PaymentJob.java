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
import uy.com.geocom.insight.dataextractor.InsightExtractorProperties;

import javax.sql.DataSource;
import java.io.File;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;

@Configuration
public class PaymentJob {

    private final InsightExtractorProperties properties;

    private final DataSource dataSource;

    private final StepBuilderFactory stepBuilderFactory;

    private final JobBuilderFactory jobBuilderFactory;

    public PaymentJob(InsightExtractorProperties properties, DataSource dataSource, StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.properties = properties;
        this.dataSource = dataSource;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    ItemReader<PaymentDTO> paymentsDatabaseReader() {
        JdbcPagingItemReader<PaymentDTO> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setPageSize(100);
        reader.setRowMapper((rs, rowNum) -> PaymentDTO.builder()
                .amount(rs.getDouble("amount"))
                .affectedAmount(rs.getDouble("affectedAmount"))
                .pos(rs.getString("pos"))
                .local(rs.getString("localId"))
                .ticketNumber(rs.getString("ticketNumber"))
                .paymentMode(rs.getString("paymentMode"))
                .isChange(rs.getBoolean("change_"))
                .client(rs.getString("clientId"))
                .date(Instant.ofEpochMilli(rs.getTimestamp("opendate")
                        .getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime())
                .build());

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT p.opendate, p.pos, p.localId, p.ticketNumber, p.amount, p.affectedAmount, p.paymentMode, p.change_, tc.clientid");
        queryProvider.setFromClause("FROM payments p LEFT JOIN ticketclients tc ON tc.pos = p.pos AND tc.ticketnumber = p.ticketnumber AND tc.localId = p.localId AND tc.opendate = p.opendate");
        HashMap<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("p.opendate", Order.DESCENDING);
        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);
        return reader;
    }

    @Bean
    ItemProcessor<PaymentDTO, PaymentDTO> paymentsProcessor() {
        return item -> item;
    }

    @Bean
    ItemWriter<PaymentDTO> paymentsCVSWriter() {
        FlatFileItemWriter<PaymentDTO> writer = new FlatFileItemWriter<>();
        writer.setHeaderCallback(writer1 -> writer1.write("DATE, POS, LOCAL, TICKET_NUMBER, AMOUNT, AFFECTED_AMOUNT, PAYMENT_MODE, CHANGE, CLIENT"));
        writer.setResource(new FileSystemResource(properties.getPath() + File.separator + "payments.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<>());

        return writer;
    }

    @Bean
    Step paymentsDBToCsvFileStep() {
        return stepBuilderFactory.get("paymentsDBToCsvFileStep")
                .<PaymentDTO, PaymentDTO>chunk(100)
                .reader(paymentsDatabaseReader())
                .processor(paymentsProcessor())
                .writer(paymentsCVSWriter())
                .build();
    }

    @Bean("payments-db-csv-job")
    Job paymentsDbToCsvFIleJob() {
        return jobBuilderFactory.get("paymentsDbToCsvFIleJob")
                .incrementer(new RunIdIncrementer())
                .flow(paymentsDBToCsvFileStep())
                .end()
                .build();
    }
}
