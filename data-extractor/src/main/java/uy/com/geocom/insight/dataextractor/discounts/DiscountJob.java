package uy.com.geocom.insight.dataextractor.discounts;

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
public class DiscountJob {

    private final InsightExtractorProperties properties;

    private final DataSource dataSource;

    private final StepBuilderFactory stepBuilderFactory;

    private final JobBuilderFactory jobBuilderFactory;

    public DiscountJob(InsightExtractorProperties properties, DataSource dataSource, StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.properties = properties;
        this.dataSource = dataSource;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    ItemReader<DiscountDTO> discountsDatabaseReader() {
        JdbcPagingItemReader<DiscountDTO> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setPageSize(100);
        reader.setRowMapper((rs, rowNum) -> DiscountDTO.builder()
                .amount(rs.getDouble("discountamount"))
                .pos(rs.getString("pos"))
                .article(rs.getString("item"))
                .quantity(rs.getInt("quantity"))
                .promotion(rs.getString("promotionId"))
                .local(rs.getString("localId"))
                .ticketNumber(rs.getString("ticketNumber"))
                .date(Instant.ofEpochMilli(rs.getTimestamp("opendate")
                        .getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime())
                .build());

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT opendate, pos, localId, ticketNumber, promotionId, item, quantity, discountamount");
        queryProvider.setFromClause("FROM discounts");
        HashMap<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("opendate", Order.DESCENDING);
        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);
        return reader;
    }

    @Bean
    ItemProcessor<DiscountDTO, DiscountDTO> discountsProcessor() {
        return item -> item;
    }

    @Bean
    ItemWriter<DiscountDTO> discountsCVSWriter() {
        FlatFileItemWriter<DiscountDTO> writer = new FlatFileItemWriter<>();
        writer.setHeaderCallback(writer1 -> writer1.write("DATE, POS, LOCAL, TICKET_NUMBER, PROMOTION, ARTICLE, QUANTITY, AMOUNT"));
        writer.setResource(new FileSystemResource(properties.getPath() + File.separator + "discounts.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<>());

        return writer;
    }

    @Bean
    Step discountsDBToCsvFileStep() {
        return stepBuilderFactory.get("discountsDBToCsvFileStep")
                .<DiscountDTO, DiscountDTO>chunk(100)
                .reader(discountsDatabaseReader())
                .processor(discountsProcessor())
                .writer(discountsCVSWriter())
                .build();
    }

    @Bean("discounts-db-csv-job")
    Job discountsDbToCsvFIleJob() {
        return jobBuilderFactory.get("discountsDbToCsvFIleJob")
                .incrementer(new RunIdIncrementer())
                .flow(discountsDBToCsvFileStep())
                .end()
                .build();
    }
}
