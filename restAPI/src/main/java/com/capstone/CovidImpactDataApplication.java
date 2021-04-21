package main.java.com.capstone;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;


@SpringBootApplication
@EnableJpaAuditing
public class CovidImpactDataApplication {

	public static void main(String[] args) {
		SpringApplication.run(CovidImpactDataApplication.class, args);
	}
}
