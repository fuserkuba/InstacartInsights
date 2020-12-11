package instacart.insights.dataextractor.articles;

import instacart.insights.dataextractor.InsightExtractorProperties;
import instacart.insights.dataextractor.TextUtils;
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
public class CategoryJob {

    private final InsightExtractorProperties properties;

    private final StepBuilderFactory stepBuilderFactory;

    private final JobBuilderFactory jobBuilderFactory;

    private final DataSource dataSource;

    public CategoryJob(InsightExtractorProperties properties, StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory, DataSource dataSource) {
        this.properties = properties;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    ItemReader<CategoryDTO> categoriesDatabaseReader() {
        JdbcPagingItemReader<CategoryDTO> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setPageSize(100);
        reader.setRowMapper(new BeanPropertyRowMapper<>(CategoryDTO.class));

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT id, name, description");
        queryProvider.setFromClause("FROM categories");
        HashMap<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.DESCENDING);
        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);
        return reader;
    }

    @Bean
    ItemProcessor<CategoryDTO, CategoryDTO> categoriesProcessor() {
        return item -> {

            if(item.getName() != null){
               item.setName(TextUtils.sanitize(item.getName()));
            }

            if (item.getDescription() != null) {
                item.setDescription(TextUtils.sanitize(item.getDescription()));
            }
            return item;
        };
    }

    @Bean
    ItemWriter<CategoryDTO> categoriesCVSWriter() {
        FlatFileItemWriter<CategoryDTO> writer = new FlatFileItemWriter<>();
        writer.setHeaderCallback(writer1 -> writer1.write("ID, NAME, DESCRIPTION"));
        writer.setResource(new FileSystemResource(properties.getPath() + File.separator + "categories.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<>());

        return writer;
    }

    @Bean
    Step categoriesDBToCsvFileStep() {
        return stepBuilderFactory.get("categoriesDBToCsvFileStep")
                .<CategoryDTO, CategoryDTO>chunk(100)
                .reader(categoriesDatabaseReader())
                .processor(categoriesProcessor())
                .writer(categoriesCVSWriter())
                .build();
    }

    @Bean("categories-db-csv-job")
    Job categoriesDbToCsvFIleJob() {
        return jobBuilderFactory.get("categoriesDbToCsvFIleJob")
                .incrementer(new RunIdIncrementer())
                .flow(categoriesDBToCsvFileStep())
                .end()
                .build();
    }
}
