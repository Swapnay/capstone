package com.capstone.controller;

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
import com.capstone.model.HousePricesData;
import com.capstone.model.HouseInventory;
import com.capstone.repository.HousingInventoryRepository;
import com.capstone.repository.HousingPricesRepository;

@RestController
@RequestMapping("/api/housing")
public class HousingDataController {

    Logger logger = LoggerFactory.getLogger(HousingDataController.class);


    @Autowired
    HousingPricesRepository housingPricesRepository;

    @Autowired
    HousingInventoryRepository housingInventoryRepository;

    @GetMapping("/prices")
    public List<HousePricesData> getAllHousePricesData() {
        logger.info("Request to get all median house prices for all states");
        return housingPricesRepository.findAll();

    }

    @GetMapping("/inventory")
    public List<HouseInventory> getAllHouseInventoryData() {
        logger.info("Request to get all median house prices for all states");
        return housingInventoryRepository.findAll();

    }

    @GetMapping("/prices/state/{state}")
    public List<HousePricesData> getHousePricesDataByState(@PathVariable(value = "state") String stateCode) {
        logger.info("Request house price data by state");
        return housingPricesRepository.findByState(stateCode)
                .orElseThrow(() -> new ResourceNotFoundException("getHousePricesDataByState", stateCode, stateCode));
    }

    @GetMapping("/inventory/state/{state}")
    public List<HouseInventory> getHouseInventoryDataByState(@PathVariable(value = "state") String stateCode) {
        logger.info("Request housing inventory data by state");
        return housingInventoryRepository.findByState(stateCode)
                .orElseThrow(() -> new ResourceNotFoundException("getHouseInventoryDataByState", stateCode, stateCode));
    }

    @GetMapping("/prices/states")
    public List<HousePricesData> getHousePricesDataByStates(@RequestParam List<String> states) {
        logger.info("Request housing price data by states");
        return housingPricesRepository.findByStateIn(states)
                .orElseThrow(() -> new ResourceNotFoundException("getHousePricesDataByStates", states.toString(), states.toString()));
    }

    @GetMapping("/inventory/states")
    public List<HouseInventory> getHouseInventoryDataByStates(@RequestParam List<String> states) {
        logger.info("Request housing inventory data by states");
        return housingInventoryRepository.findByStateIn(states)
                .orElseThrow(() -> new ResourceNotFoundException("getHouseInventoryDataByStates", states.toString(), states.toString()));
    }


}
