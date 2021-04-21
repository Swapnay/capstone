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
import com.capstone.model.StockLine;
import com.capstone.repository.StocksRepository;

@RestController
@RequestMapping("/api/stocks")
public class StocksController {


    Logger logger = LoggerFactory.getLogger(StocksController.class);


    @Autowired
    StocksRepository stocksRepository;

    @GetMapping("/")
    public List<StockLine> getAllStocksData() {
        logger.info("request for all s&p stocks");
        return stocksRepository.findAll();

    }

    @GetMapping("/{ticker}")
    public List<StockLine> getDataBySymbol(@PathVariable(value = "ticker") String symbol) {
        logger.info(String.format("Request for stock data with symbol: %s ",symbol));
        return stocksRepository.findBySymbol(symbol)
                .orElseThrow(() -> new ResourceNotFoundException("StockLine", "symbol", symbol));
    }

    @GetMapping("/tickers")
    public List<StockLine> getDataBySymbols(@RequestParam List<String> symbols) {
        logger.info(String.format("Request for stock data with symbols %s ",symbols));
        return stocksRepository.findBySymbolIn(symbols)
                .orElseThrow(() -> new ResourceNotFoundException("StockLine", "symbols", symbols));
    }
}
