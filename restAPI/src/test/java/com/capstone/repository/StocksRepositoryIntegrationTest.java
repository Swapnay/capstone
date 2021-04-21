package test.java.com.capstone.repository;

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

import com.capstone.model.CountryMonthlyData;
import com.capstone.model.StockLine;

import static org.assertj.core.api.Assertions.*;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource(
        locations = "classpath:application-integrationtest.properties")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class StocksRepositoryIntegrationTest {



    @Autowired
    private StocksRepository repository;


    @Test
    public void findBySymbol() {
        Optional<List<StockLine>> monthly  = repository.findBySymbol("ZZZ");
        monthly.ifPresent(monthlyDataList -> {
            List<StockLine> yearlyList = monthlyDataList.stream()
                                                      .filter(stm->stm.getYear()==2020)
                                                      .collect(Collectors.toList());
            assertThat(yearlyList.size() == 2);
                });

    }

    @Test
    public void findBySymbolIn() {
        Optional<List<StockLine>> monthly  = repository.findBySymbolIn(Arrays.asList("MMM","A","AAPL","AOS"));
        monthly.ifPresent(monthlyDataList -> {
            List<StockLine> yearlyList = monthlyDataList.stream()
                                                          .filter(stm->stm.getYear() == 2020)
                                                          .collect(Collectors.toList());
            assertThat(yearlyList.size() == 2);
        });

    }

}
