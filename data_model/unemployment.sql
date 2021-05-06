USE covid_economy_impact;

drop table unemployment_by_race_fact;
CREATE TABLE unemployment_by_race_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    race_type ENUM('white','african american','asian','hispanic'),
    civilian_noninstitutional MEDIUMINT,
    participated  MEDIUMINT,
    participated_rate  DIIMAL(4,2),
    unemployed MEDIUMINT,
    unemployed_rate  DIIMAL(4,2),
    submission_date TIMESTAMP NOT NULL,
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id),
    UNIQUE(date_id,race_type)
)ENGINE=InnoDB;

drop table unemployment_by_education_level_fact;
CREATE TABLE unemployment_by_education_level_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    education ENUM('less than high school', 'high school grad','some college associate',
    'bachelors or higher college associate'),
    participated  MEDIUMINT,
    participated_rate  DIIMAL(4,2),
    unemployed MEDIUMINT,
    unemployed_rate  DIIMAL(4,2),
    submission_date TIMESTAMP NOT NULL,
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id),
    UNIQUE(date_id,education)
)ENGINE=InnoDB;

CREATE TABLE unemployment_rate_by_state_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    state_id BIGINT UNSIGNED,
    unemployment_rate DIIMAL(4,2),
    submission_date TIMESTAMP NOT NULL,
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id),
    FOREIGN KEY (state_id) REFERENCES state_dim(id),
    UNIQUE(date_id,state_id)
)ENGINE=InnoDB;


CREATE TABLE unemployment_by_industry_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    industry_type ENUM('total','construction','manufacturing','whole sale trade',
    'transportation and utilities', 'professional business', 'education health care',
    'leisure hospitality', 'agriculture related' ),
    unemployment MEDIUMINT,
    unemployment_rate DIIMAL(4,2),
    submission_date TIMESTAMP NOT NULL,
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id),
    UNIQUE(date_id,industry_type)
)ENGINE=InnoDB;

DROP TABLE IF EXISTS `unemployment_monthly`;
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




