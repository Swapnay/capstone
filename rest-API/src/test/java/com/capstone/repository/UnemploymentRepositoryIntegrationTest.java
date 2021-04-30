package  com.capstone.repository;

import java.util.Arrays;
import java.util.Collection;
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

import com.capstone.model.UnemploymentRateData;

import static org.assertj.core.api.Assertions.*;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource(
        locations = "classpath:application-integrationtest.properties")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class UnemploymentRepositoryIntegrationTest {



    @Autowired
    private UnemploymentRateRepository repository;

    @Test
    public void findByVariableType() {
        Optional<List<UnemploymentRateData>> monthly  = repository.findByVariableType("race");
        monthly.ifPresent(monthlyDataList -> {
            List<UnemploymentRateData> caYearlyList = monthlyDataList.stream()
                                                                      .filter(stm->stm.getYear()==2020)
                                                                      .collect(Collectors.toList());
            assertThat(caYearlyList.size() == 48);
                });

    }

    @Test
    public void findByVariableNameIn() {
        Optional<List<UnemploymentRateData>> monthly  = repository.findByVariableNameIn(Arrays.asList("CA","AZ","IN"));
        monthly.ifPresent(monthlyDataList -> {
            List<UnemploymentRateData> caYearlyList = monthlyDataList.stream()
                                                                      .filter(stm->stm.getYear() == 2020)
                                                                      .collect(Collectors.toList());
            assertThat(caYearlyList.size() == 33);
        });

    }

}
