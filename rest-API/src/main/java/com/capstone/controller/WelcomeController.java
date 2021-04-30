package  com.capstone.controller;


import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class WelcomeController{

    Logger logger = LoggerFactory.getLogger(WelcomeController.class);

    @GetMapping
    public String sayHello() {
        logger.info("Covid Effect on different sectors API is running");
        return "Covid-Sector Data App Is UP";
    }


}
