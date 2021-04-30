package  com.capstone.repository;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;


import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.capstone.model.StateMonthlyData;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource(
        locations = "classpath:application-integrationtest.properties")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class CovidUSARepositoryIntegrationTest {



    @Autowired
    private CovidUSARepository covidUSARepository;


    @Test
    public void findByState() {
        Optional<List<StateMonthlyData>> monthly  = covidUSARepository.findByState("AZ");
        monthly.ifPresent(stateMonthlyDataList -> {
            List<StateMonthlyData> caYearlyList = stateMonthlyDataList.stream()
                                                                      .filter(stm->stm.getYear()==2020)
                                                                      .collect(Collectors.toList());
            assertThat(caYearlyList.size() == 1);
                });

    }

    @Test
    public void findByStateIn() {
        Optional<List<StateMonthlyData>> monthly  = covidUSARepository.findByStateIn(Arrays.asList("CA","AZ","IN"));
        monthly.ifPresent(stateMonthlyDataList -> {
            List<StateMonthlyData> caYearlyList = stateMonthlyDataList.stream()
                                                                      .filter(stm->stm.getYear() == 2020)
                                                                      .collect(Collectors.toList());
            assertThat(caYearlyList.size() == 2);
        });

    }

}
