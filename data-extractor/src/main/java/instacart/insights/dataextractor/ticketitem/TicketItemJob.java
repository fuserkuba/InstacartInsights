package instacart.insights.dataextractor.ticketitem;

import instacart.insights.dataextractor.InsightExtractorProperties;
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

import javax.sql.DataSource;
import java.io.File;
import java.util.HashMap;

@Configuration
public class TicketItemJob {

    private final InsightExtractorProperties properties;

    private final DataSource dataSource;

    private final StepBuilderFactory stepBuilderFactory;

    private final JobBuilderFactory jobBuilderFactory;

    public TicketItemJob(InsightExtractorProperties properties, DataSource dataSource, StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.properties = properties;
        this.dataSource = dataSource;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    ItemReader<TicketItemDTO> ticketItemDatabaseReader() {
        JdbcPagingItemReader<TicketItemDTO> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setPageSize(100);
        reader.setRowMapper(new BeanPropertyRowMapper<>(TicketItemDTO.class));

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT id, item AS articles, quantity, amount, itemtype AS type");
        queryProvider.setFromClause("FROM ticketitems");
        HashMap<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.DESCENDING);
        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);
        return reader;
    }

    @Bean
    ItemWriter<TicketItemDTO> ticketItemCVSWriter() {
        FlatFileItemWriter<TicketItemDTO> writer = new FlatFileItemWriter<>();
        writer.setHeaderCallback(writer1 -> writer1.write("ID, ARTICLE, QUANTITY, AMOUNT, TYPE"));
        writer.setResource(new FileSystemResource(properties.getPath() + File.separator + "ticket-items.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<>());

        return writer;
    }

    @Bean
    Step ticketItemDBToCsvFileStep() {
        return stepBuilderFactory.get("ticketItemDBToCsvFileStep")
                .<TicketItemDTO, TicketItemDTO>chunk(100)
                .reader(ticketItemDatabaseReader())
                .processor((ItemProcessor<TicketItemDTO, TicketItemDTO>) item -> item)
                .writer(ticketItemCVSWriter())
                .build();
    }

    @Bean("ticketitems-db-csv-job")
    Job ticketItemDbToCsvFIleJob() {
        return jobBuilderFactory.get("ticketItemDbToCsvFIleJob")
                .incrementer(new RunIdIncrementer())
                .flow(ticketItemDBToCsvFileStep())
                .end()
                .build();
    }
}
