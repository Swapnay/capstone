package main.java.com.capstone.controller;

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
import com.capstone.model.StateMonthlyData;
import com.capstone.repository.CovidUSARepository;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@RequestMapping("/api/covid/usa")
public class CovidUSAController {


    Logger logger = LoggerFactory.getLogger(CovidUSAController.class);


    @Autowired
    CovidUSARepository covidUSARepository;
    @ApiOperation(value = "Get a list of available applications.")
    @ApiResponses({
            @ApiResponse(code = 200, message = "200", response = StateMonthlyData.class)
    })
    @GetMapping("/")
    public List<StateMonthlyData> getAllUSAStatesData() {
        logger.info("Request for all usa data");
        return covidUSARepository.findAll();

    }

    @GetMapping("/states/{state}")
    public List<StateMonthlyData> getCovidStateMonthlyDataByState(@PathVariable(value = "state") String stateCode) {
        logger.info(String.format("Finding usa data details by state: %s ",stateCode));
        return covidUSARepository.findByState(stateCode)
                .orElseThrow(() -> new ResourceNotFoundException("StateMonthlyData", "state", stateCode));
    }

    @GetMapping("/states")
    public List<StateMonthlyData> getCovidStateMonthlyDataByStates(@RequestParam List<String> states) {
        logger.info(String.format("Finding states data %s ",states));
        return covidUSARepository.findByStateIn(states)
                .orElseThrow(() -> new ResourceNotFoundException("StateMonthlyData", "state", states));
    }

}
