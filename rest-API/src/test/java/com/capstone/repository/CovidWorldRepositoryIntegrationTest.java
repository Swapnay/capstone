package  com.capstone.repository;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.capstone.model.CountryMonthlyData;

import static org.assertj.core.api.Assertions.*;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource(
        locations = "classpath:application-integrationtest.properties")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class CovidWorldRepositoryIntegrationTest {



    @Autowired
    private CovidWorldRepository repository;


    @Test
    public void findByCountry() {
        Optional<List<CountryMonthlyData>> monthly  = repository.findByCountry("MUS");
        monthly.ifPresent(monthlyDataList -> {
            List<CountryMonthlyData> yearlyList = monthlyDataList.stream()
                                                                      .filter(stm->stm.getYear()==2020)
                                                                      .collect(Collectors.toList());
            assertThat(yearlyList.size() == 2);
                });

    }

    @Test
    public void findByCountryIn() {
        Optional<List<CountryMonthlyData>> monthly  = repository.findByCountryIn(Arrays.asList("MUS","MNG"));
        monthly.ifPresent(monthlyDataList -> {
            List<CountryMonthlyData> caYearlyList = monthlyDataList.stream()
                                                                      .filter(stm->stm.getYear() == 2020)
                                                                      .collect(Collectors.toList());
            assertThat(caYearlyList.size() == 3);
        });

    }

}
