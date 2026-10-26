package io.github.chris316.taxidemand;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class TaxiDemandApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(TaxiDemandApplication.class, args);
        // The replayer is a batch job: once it has sent the file, shut down instead of idling.
        if (context.getEnvironment().matchesProfiles("replayer")) {
            System.exit(SpringApplication.exit(context));
        }
    }
}
