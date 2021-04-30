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
import com.capstone.model.UnemploymentRateData;
import com.capstone.repository.UnemploymentRateRepository;

@RestController
@RequestMapping("/api/unemploymentrate")
public class UnemploymentRateController {
    public static final String race = "race";
    public  static  final String industry = "industry";
    public  static  final String state = "state";

    Logger logger = LoggerFactory.getLogger(UnemploymentRateController.class);


    @Autowired
    UnemploymentRateRepository unemploymentRateRepository;

    @GetMapping("/")
    public List<UnemploymentRateData> getAllUnemploymentRateData() {
        logger.info("Request to get all unemployment data");
        return unemploymentRateRepository.findAll();

    }

    @GetMapping("/race")
    public List<UnemploymentRateData> getUnemploymentRateDataRace() {
        logger.info("Request unemployment data by race");
        return unemploymentRateRepository.findByVariableType(race)
                .orElseThrow(() -> new ResourceNotFoundException("UnemploymentRateData", race, race));
    }

    @GetMapping("/industry")
    public List<UnemploymentRateData> getUnemploymentRateDataIndustry() {
        logger.info("Request unemployment data by industry");
        return unemploymentRateRepository.findByVariableType(industry)
                .orElseThrow(() -> new ResourceNotFoundException("UnemploymentRateData", industry, industry));
    }

    @GetMapping("/state")
    public List<UnemploymentRateData> getUnemploymentRateDataByState() {
        logger.info("Request unemployment data by state");
        return unemploymentRateRepository.findByVariableType(state)
                .orElseThrow(() -> new ResourceNotFoundException("UnemploymentRateData", state, state));
    }

    @GetMapping("/states")
    public List<UnemploymentRateData> getUnemploymentRateDataByStates(@RequestParam List<String> states) {
        logger.info("Request unemployment data by state");
        return unemploymentRateRepository.findByVariableNameIn(states)
                .orElseThrow(() -> new ResourceNotFoundException("UnemploymentRateData", state, state));
    }

}
