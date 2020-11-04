USE covid_economy_impact;
CREATE TABLE unemployment_by_race_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    civilian_noninstitutional_white  MEDIUMINT,
    white_participated  MEDIUMINT,
    white_participated_rate  DECIMAL(4,2),
    white_unemployed MEDIUMINT,
    white_unemployed_rate  DECIMAL(4,2),
    civilian_noninstitutional_black MEDIUMINT,
    black_participated  MEDIUMINT,
    black_participated_rate  DECIMAL(4,2),
    black_unemployed MEDIUMINT,
    black_unemployed_rate  DECIMAL(4,2),
    civilian_noninstitutional_hlo MEDIUMINT,
    hlo_participated  MEDIUMINT,
    hlo_participated_rate  DECIMAL(4,2),
    hlo_unemployed MEDIUMINT,
    hlo_unemployed_rate  DECIMAL(4,2),
    civilian_noninstitutional_asian MEDIUMINT,
    asian_participated  MEDIUMINT,
    asian_participated_rate  DECIMAL(4,2),
    asian_unemployed MEDIUMINT,
    asian_unemployed_rate  DECIMAL(4,2),
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id)
)ENGINE=InnoDB;

CREATE TABLE unemployment_by_education_level_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    less_than_high_school_participated  MEDIUMINT,
    less_than_high_school_participated_rate  DECIMAL(4,2),
    less_than_high_school_unemployed MEDIUMINT,
    less_than_high_school_unemployed_rate  DECIMAL(4,2),
    high_school_grad_participated  MEDIUMINT,
    high_school_grad_participated_rate  DECIMAL(4,2),
    high_school_grad_unemployed MEDIUMINT,
    high_school_grad_unemployed_rate  DECIMAL(4,2),
    some_college_associate_participated  MEDIUMINT,
    some_college_associate_participated_rate  DECIMAL(4,2),
    some_college_associate_unemployed MEDIUMINT,
    some_college_associate_unemployed_rate  DECIMAL(4,2),
    bachelors_or_higher_college_associate_participated  MEDIUMINT,
    bachelors_or_higher_college_associate_participated_rate  DECIMAL(4,2),
    bachelors_or_higher_college_associate_unemployed MEDIUMINT,
    bachelors_or_higher_college_associate_unemployed_rate  DECIMAL(4,2),
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id)
)ENGINE=InnoDB;
INSERT INTO unemployment_by_education_level_fact( date_id, less_than_high_school_participated, less_than_high_school_participated_rate, less_than_high_school_unemployed, less_than_high_school_unemployed_rate, high_school_grad_participated, high_school_grad_participated_rate, high_school_grad_unemployed, high_school_grad_unemployed_rate, some_college_associate_participated, some_college_associate_participated_rate, some_college_associate_unemployed, some_college_associate_unemployed_rate, bachelors_or_higher_college_associate_participated, bachelors_or_higher_college_associate_participated_rate, bachelors_or_higher_college_associate_unemployed, bachelors_or_higher_college_associate_unemployed_rate)
VALUES( :date_id, :less_than_high_school_participated, :less_than_high_school_participated_rate, :less_than_high_school_unemployed, :less_than_high_school_unemployed_rate, :high_school_grad_participated, :high_school_grad_participated_rate, :high_school_grad_unemployed, :high_school_grad_unemployed_rate, :some_college_associate_participated, :some_college_associate_participated_rate, :some_college_associate_unemployed, :some_college_associate_unemployed_rate, :bachelors_or_higher_college_associate_participated, :bachelors_or_higher_college_associate_participated_rate, :bachelors_or_higher_college_associate_unemployed, :bachelors_or_higher_college_associate_unemployed_rate)


CREATE TABLE unemployment_by_industry_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    total_unemployment MEDIUMINT,
    unemployment_rate DECIMAL(4,2),
    construction MEDIUMINT,
    construction_rate DECIMAL(4,2),
    manufacturing MEDIUMINT,
    manufacturing_rate DECIMAL(4,2),
    whole_sale_trade MEDIUMINT,
    whole_sale_trade_rate DECIMAL(4,2),
    transportation_and_utilities MEDIUMINT,
    transportation_and_utilities_rate DECIMAl(4,2),
    professional_business  MEDIUMINT,
    professional_business_rate DECIMAL(4,2),
    education_health_care MEDIUMINT,
    education_health_care_rate DECIMAL(4,2),
    leisure_hospitality MEDIUMINT,
    leisure_hospitality_rate DECIMAL(4,2),
    agriculture_related MEDIUMINT,
    agriculture_related_rate DECIMAL(4,2),
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id)
)ENGINE=InnoDB;
