USE covid_economy_impact;
drop table unemployment_by_race_fact;
CREATE TABLE unemployment_by_race_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    civilian_noninstitutional_white  MEDIUMINT,
    white_participated  MEDIUMINT,
    white_participated_rate  DIIMAL(4,2),
    white_unemployed MEDIUMINT,
    white_unemployed_rate  DIIMAL(4,2),
    civilian_noninstitutional_black MEDIUMINT,
    black_participated  MEDIUMINT,
    black_participated_rate  DIIMAL(4,2),
    black_unemployed MEDIUMINT,
    black_unemployed_rate  DIIMAL(4,2),
    civilian_noninstitutional_hlo MEDIUMINT,
    hlo_participated  MEDIUMINT,
    hlo_participated_rate  DIIMAL(4,2),
    hlo_unemployed MEDIUMINT,
    hlo_unemployed_rate  DIIMAL(4,2),
    civilian_noninstitutional_asian MEDIUMINT,
    asian_participated  MEDIUMINT,
    asian_participated_rate  DIIMAL(4,2),
    asian_unemployed MEDIUMINT,
    asian_unemployed_rate  DIIMAL(4,2),
    submission_date TIMESTAMP NOT NULL,
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id)
)ENGINE=InnoDB;

drop table unemployment_by_education_level_fact;
CREATE TABLE unemployment_by_education_level_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    less_than_high_school_participated  MEDIUMINT,
    less_than_high_school_participated_rate  DIIMAL(4,2),
    less_than_high_school_unemployed MEDIUMINT,
    less_than_high_school_unemployed_rate  DIIMAL(4,2),
    high_school_grad_participated  MEDIUMINT,
    high_school_grad_participated_rate  DIIMAL(4,2),
    high_school_grad_unemployed MEDIUMINT,
    high_school_grad_unemployed_rate  DIIMAL(4,2),
    some_college_associate_participated  MEDIUMINT,
    some_college_associate_participated_rate  DIIMAL(4,2),
    some_college_associate_unemployed MEDIUMINT,
    some_college_associate_unemployed_rate  DIIMAL(4,2),
    bachelors_or_higher_college_associate_participated  MEDIUMINT,
    bachelors_or_higher_college_associate_participated_rate  DIIMAL(4,2),
    bachelors_or_higher_college_associate_unemployed MEDIUMINT,
    bachelors_or_higher_college_associate_unemployed_rate  DIIMAL(4,2),
    submission_date TIMESTAMP NOT NULL,
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id)
)ENGINE=InnoDB;

drop table unemployment_by_industry_fact;
CREATE TABLE unemployment_by_industry_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    total_unemployment MEDIUMINT,
    unemployment_rate DIIMAL(4,2),
    construction MEDIUMINT,
    construction_rate DIIMAL(4,2),
    manufacturing MEDIUMINT,
    manufacturing_rate DIIMAL(4,2),
    whole_sale_trade MEDIUMINT,
    whole_sale_trade_rate DIIMAL(4,2),
    transportation_and_utilities MEDIUMINT,
    transportation_and_utilities_rate DIIMAl(4,2),
    professional_business  MEDIUMINT,
    professional_business_rate DIIMAL(4,2),
    education_health_care MEDIUMINT,
    education_health_care_rate DIIMAL(4,2),
    leisure_hospitality MEDIUMINT,
    leisure_hospitality_rate DIIMAL(4,2),
    agriculture_related MEDIUMINT,
    agriculture_related_rate DIIMAL(4,2),
    submission_date TIMESTAMP NOT NULL,
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id)
)ENGINE=InnoDB;
drop table unemployment_by_race_normalized_fact;
CREATE TABLE unemployment_by_race_normalized_fact
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

drop table unemployment_by_education_level_normalized_fact;
CREATE TABLE unemployment_by_education_level_normalized_fact
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


CREATE TABLE unemployment_by_industry_normalized_fact
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






