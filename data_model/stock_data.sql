USE covid_economy_impact;
CREATE TABLE stocks_dim
(
     id SERIAL PRIMARY KEY,
     symbol VARCHAR(10) UNIQUE,
     name VARCHAR(50),
     category_name VARCHAR(40)

)ENGINE=InnoDB;


CREATE TABLE stock_prices_fact
(
    id SERIAL PRIMARY KEY,
    stock_id BIGINT UNSIGNED,
    date_id BIGINT UNSIGNED,
    stock_date timestamp NOT NULL,
    open_price DECIMAL(6,2),
    closing_price DECIMAL(6,2),
    low DECIMAL(6,2),
    high DECIMAL(6,2),
    volume BIGINT,
    UNIQUE(stock_id,date_id),
    FOREIGN KEY (date_id) REFERENCES covid_date_dim(id),
    FOREIGN KEY (stock_id) REFERENCES stocks_dim(id)

)ENGINE=InnoDB;
CREATE TABLE stocks_monthly_avg_return
(
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    category_name VARCHAR(40) NOT NULL,
    year INT NOT NULL,
    month TiNYINT,
    return_rate FLOAT,
    cosing_price FLOAT,
    UNIQUE(symbol, month,year)
)ENGINE=InnoDB;
DROP TABLE IF EXISTS `stocks_monthly`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `stocks_monthly` (
  `symbol` text NOT NULL,
  `category_name` text NOT NULL,
  `year` int DEFAULT NULL,
  `month` int DEFAULT NULL,
  `monthly_return_rate` double DEFAULT NULL,
  `monthly_avg` double DEFAULT NULL,
  `id` bigint NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
);


