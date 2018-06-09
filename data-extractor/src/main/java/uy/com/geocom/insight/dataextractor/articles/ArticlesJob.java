package uy.com.geocom.insight.dataextractor.articles;

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
import uy.com.geocom.insight.dataextractor.TextUtils;

import javax.sql.DataSource;
import java.io.File;
import java.util.HashMap;

@Configuration
public class ArticlesJob {

    private final InsightExtractorProperties properties;

    private final StepBuilderFactory stepBuilderFactory;

    private final JobBuilderFactory jobBuilderFactory;

    private final DataSource dataSource;

    public ArticlesJob(InsightExtractorProperties properties, StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory, DataSource dataSource) {
        this.properties = properties;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    ItemReader<ArticleDTO> articleDatabaseReader() {
        JdbcPagingItemReader<ArticleDTO> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setPageSize(100);
        reader.setRowMapper(new BeanPropertyRowMapper<>(ArticleDTO.class));

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT a.id, a.description, ic.category");
        queryProvider.setFromClause("FROM articles a LEFT JOIN itemcategories ic ON  a.id = ic.item");
        HashMap<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.DESCENDING);
        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);
        return reader;
    }

    @Bean
    ItemProcessor<ArticleDTO, ArticleDTO> articleProcessor() {
        return item -> {
            if (item.getDescription() != null) {
                item.setDescription(TextUtils.sanitize(item.getDescription()));
            }
            return item;
        };
    }

    @Bean
    ItemWriter<ArticleDTO> articleCVSWriter() {
        FlatFileItemWriter<ArticleDTO> writer = new FlatFileItemWriter<>();
        writer.setHeaderCallback(writer1 -> writer1.write("ID, DESCRIPTION, CATEGORY"));
        writer.setResource(new FileSystemResource(properties.getPath() + File.separator + "articles.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<>());

        return writer;
    }

    @Bean
    Step articlesDBToCsvFileStep() {
        return stepBuilderFactory.get("articlesDBToCsvFileStep")
                .<ArticleDTO, ArticleDTO>chunk(100)
                .reader(articleDatabaseReader())
                .processor(articleProcessor())
                .writer(articleCVSWriter())
                .build();
    }

    @Bean("articles-db-csv-job")
    Job articlesDbToCsvFIleJob() {
        return jobBuilderFactory.get("articlesDbToCsvFIleJob")
                .incrementer(new RunIdIncrementer())
                .flow(articlesDBToCsvFileStep())
                .end()
                .build();
    }
}
