package  com.capstone.config;

import javax.servlet.ServletContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.google.common.base.Predicate;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.paths.AbstractPathProvider;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import static com.google.common.base.Predicates.*;

import static springfox.documentation.builders.PathSelectors.*;

@Configuration
@EnableSwagger2
//@EnableWebMvc
public class SpringFoxConfig extends AbstractPathProvider  {




    public static final String ROOT = "/";


    @Bean
    public Docket postsApi() {
        return new Docket(DocumentationType.SWAGGER_2).groupName("public-api")
                .apiInfo(apiInfo()).select().paths(postPaths()).build().pathProvider(this);
    }

    private Predicate<String> postPaths() {
        return or(regex("/*"),
                regex("/api/covid/usa/*"),
                regex("/api/covid/usa/*"),
                regex("/api/covid/usa/states"),
                regex("/api/covid/usa/states/\\{state\\}"),
                regex("/api/covid/world/*"),
                regex("/api/covid/world/*"),
                regex("/api/covid/world/countries/\\{country\\}"),
                regex("/api/covid/world/countries"),
                regex("/api/covid/usa/states/\\{state\\}"),
                regex("/api/stocks/*"),
                regex("/api/stocks/tickers"),
                regex("/api/stocks/\\{ticker\\}"),
                regex("/api/unemploymentrate/*"),
                regex("/api/unemploymentrate/race"),
                regex("/api/unemploymentrate/industry"),
                regex("/api/unemploymentrate/state"),
                regex("/api/unemploymentrate/states"),
                regex("/api/housing/prices"),
                regex("/api/housing/inventory"),
                regex("/api/housing/prices/state/\\{state\\}"),
                regex("/api/housing/inventory/state/\\{state\\}"),
                regex("/api/housing/prices/states"),
                regex("/api/housing/inventory/states"));
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder().title("COVID-Effect API")
                .description("Api to get covid and other sector data")
                .contact("syeruvala@gmail.com")
                .version("1.0").build();
    }
    @Override
    protected String applicationPath() {
        return "/";
    }

    @Override
    protected String getDocumentationPath() {
        return "/";
    }
}
