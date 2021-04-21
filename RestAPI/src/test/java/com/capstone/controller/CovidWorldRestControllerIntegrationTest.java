package test.java.com.capstone.controller;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import com.capstone.CovidImpactDataApplication;
import com.capstone.repository.CovidWorldRepository;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment=SpringBootTest.WebEnvironment.MOCK,
        classes = CovidImpactDataApplication.class)
@AutoConfigureMockMvc
@TestPropertySource(
        locations = "classpath:application-integrationtest.properties")

//@EnableAutoConfiguration(exclude=SecurityAutoConfiguration.class)

public class CovidWorldRestControllerIntegrationTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private CovidWorldRepository repository;

//    @After
//    public void resetDb() {
//        repository.deleteAll();
//    }

    @Test
    public void getAllCountriesMonthlyData() throws IOException, Exception {
        mvc.perform(get("/api/covid/world/").contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(10))));
    }

    @Test
    public void getCovidMonthlyDataByCountry() throws Exception {

        mvc.perform(get("/api/covid/world/countries/MUS").contentType(MediaType.APPLICATION_JSON))
          .andDo(print())
          .andExpect(status().isOk())
          .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
          .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(2))));

    }

    @Test
    public void getCovidStateMonthlyDataBycountries() throws Exception {

        mvc.perform(get("/api/covid/world/countries?countries=MUS,AUT").contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(3))));

    }

}
