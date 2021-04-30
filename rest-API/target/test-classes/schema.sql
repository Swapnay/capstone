use capstone_testing;
DROP TABLE IF EXISTS `covid_usa_monthly`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `covid_usa_monthly` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `state` varchar(50) NOT NULL,
  `month` tinyint NOT NULL,
  `year` int NOT NULL,
  `avg_new_cases` float DEFAULT NULL,
  `monthly_new_cases` float DEFAULT NULL,
  `avg_new_deaths` float DEFAULT NULL,
  `monthly_new_deaths` float DEFAULT NULL,
  `total_cases` float DEFAULT NULL,
  `total_deaths` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `state` (`state`,`month`,`year`)
) ;
DROP TABLE IF EXISTS `covid_world_monthly`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `covid_world_monthly` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `country` varchar(50) NOT NULL,
  `country_name` varchar(50) NOT NULL,
  `month` tinyint NOT NULL,
  `year` int NOT NULL,
  `avg_new_cases` float DEFAULT NULL,
  `monthly_new_cases` float DEFAULT NULL,
  `avg_new_deaths` float DEFAULT NULL,
  `monthly_new_deaths` float DEFAULT NULL,
  `monthly_tests` float DEFAULT NULL,
  `total_cases` float DEFAULT NULL,
  `total_deaths` float DEFAULT NULL,
  `population` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `country` (`country`,`month`,`year`)
);
DROP TABLE IF EXISTS `unemployment_monthly`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `unemployment_monthly` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `variable_type` varchar(50) NOT NULL,
  `variable_name` varchar(40) NOT NULL,
  `year` int NOT NULL,
  `month` tinyint DEFAULT NULL,
  `submission_date` date DEFAULT NULL,
  `unemployed_rate` decimal(4,2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `variable_type` (`variable_type`,`variable_name`,`month`,`year`)
);
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