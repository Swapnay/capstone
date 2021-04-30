package  com.capstone.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.capstone.exception.ResourceNotFoundException;
import com.capstone.model.CountryMonthlyData;
import com.capstone.repository.CovidWorldRepository;


@RestController
@RequestMapping("/api/covid/world")
public class CovidWorldController {
    Logger logger = LoggerFactory.getLogger(CovidUSAController.class);
    @Autowired
    CovidWorldRepository covidWorldRepository;

    @GetMapping("/")
    public List<CountryMonthlyData> getAllCountriesMonthlyData() {
        logger.info("Request is for all countries monthly data");
        return covidWorldRepository.findAll();

    }

    @GetMapping("/countries/{country}")
    public List<CountryMonthlyData> getCovidMonthlyDataByCountry(@PathVariable(value = "country") String country) {
        logger.info(String.format("Request for world data details by country: %s ",country));
        return covidWorldRepository.findByCountry(country)
                .orElseThrow(() -> new ResourceNotFoundException("CountryMonthlyData", "country", country));
    }

    @GetMapping("/countries")
    public List<CountryMonthlyData> getCovidMonthlyDataByCountries(@RequestParam List<String> countries) {
        logger.info(String.format("Request for  world data details by countries %s ",countries));
        return covidWorldRepository.findByCountryIn(countries)
                .orElseThrow(() -> new ResourceNotFoundException("CountryMonthlyData", "country", countries));
    }





}
