package instacart.insights.dataextractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.HashSet;
import java.util.Set;

@SpringBootApplication
@EnableConfigurationProperties({InsightExtractorProperties.class})
public class InsightDataExtractorApplication implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsightDataExtractorApplication.class);

    private final Job articlesJob;

    private final Job paymentsJob;

    private final Job ticketItemJob;

    private final Job discountsJob;

    private final Job categoriesJob;

    private final Job paymentModesJob;

    private final JobLauncher jobLauncher;

    public InsightDataExtractorApplication(@Qualifier("articles-db-csv-job") Job articlesJob,
                                           @Qualifier("payments-db-csv-job") Job paymentsJob,
                                           @Qualifier("ticketitems-db-csv-job") Job ticketItemJob,
                                           @Qualifier("discounts-db-csv-job") Job discountsJob,
                                           @Qualifier("categories-db-csv-job") Job categoriesJob,
                                           @Qualifier("payment-modes-db-csv-job") Job paymentModesJob,
                                           JobLauncher jobLauncher) {
        this.articlesJob = articlesJob;
        this.paymentsJob = paymentsJob;
        this.ticketItemJob = ticketItemJob;
        this.discountsJob = discountsJob;
        this.categoriesJob = categoriesJob;
        this.jobLauncher = jobLauncher;
        this.paymentModesJob = paymentModesJob;
    }

    public static void main(String[] args) {
        SpringApplication.run(InsightDataExtractorApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args){

        Set<Job> jobs = new HashSet<>();

        //run all jobs
        if (args.getOptionNames().isEmpty()) {
            jobs.add(articlesJob);
            jobs.add(paymentsJob);
            jobs.add(ticketItemJob);
            jobs.add(discountsJob);
            jobs.add(categoriesJob);
            jobs.add(paymentModesJob);
        }
        else if (args.getOptionValues("articles") != null) {
            jobs.add(articlesJob);
            jobs.add(categoriesJob);
        }

        if (args.getOptionValues("payments") != null) {
            jobs.add(paymentsJob);
            jobs.add(paymentModesJob);
        }

        if (args.getOptionValues("tickets") != null)
            jobs.add(ticketItemJob);

        if (args.getOptionValues("discounts") != null)
            jobs.add(discountsJob);



        jobs.forEach(job -> {
            try {
                jobLauncher.run(job, new JobParameters());
            } catch (JobExecutionAlreadyRunningException | JobParametersInvalidException | JobInstanceAlreadyCompleteException | JobRestartException e) {
               LOGGER.error("Cannot execute the job '{}'. An exception thrown: {}", job.getName(), e.getMessage());
            }
        });
    }
}
