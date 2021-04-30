package  com.capstone.controller;

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
import com.capstone.repository.UnemploymentRateRepository;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.*;
@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment=SpringBootTest.WebEnvironment.MOCK,
        classes = CovidImpactDataApplication.class)
@AutoConfigureMockMvc
@TestPropertySource(
        locations = "classpath:application-integrationtest.properties")

//@EnableAutoConfiguration(exclude=SecurityAutoConfiguration.class)

public class UnemploymentRestControllerIntegrationTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private UnemploymentRateRepository repository;

//    @After
//    public void resetDb() {
//        repository.deleteAll();
//    }

    @Test
    public void getAllUnemploymentRateData() throws IOException, Exception {
        mvc.perform(get("/api/unemploymentrate/")
                .with(user("admin")
                .password("admin")
                .roles("USER","ADMIN"))
                .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(1594))));
    }

    @Test
    public void getUnemploymentRateDataRace() throws Exception {

        mvc.perform(get("/api/unemploymentrate/race")
                .with(user("admin")
                .password("admin")
                .roles("USER","ADMIN"))
                .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(108))));

    }

    @Test
    public void getUnemploymentRateDataIndustry() throws Exception {

        mvc.perform(get("/api/unemploymentrate/industry")
                .with(user("admin")
                .password("admin")
                .roles("USER","ADMIN"))
                .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(216))));

    }

    @Test
    public void getCovidStateMonthlyDataBycountries() throws Exception {

        mvc.perform(get("/api/unemploymentrate/states?states=CA,AZ,IN")
                .with(user("admin")
                .password("admin")
                .roles("USER","ADMIN"))
                .contentType(MediaType.APPLICATION_JSON))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", hasSize(greaterThanOrEqualTo(33))));

    }

}
