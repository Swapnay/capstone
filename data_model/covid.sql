DROP DATABASE IF EXISTS covid_economy_impact;

CREATE DATABASE covid_economy_impact;
USE covid_economy_impact;
CREATE TABLE covid_date_dim
(
      id SERIAL PRIMARY KEY,
      day TINYINT,
      month TINYINT,
      year SMALLINT
)ENGINE=InnoDB;
CREATE TABLE country_dim
(
    id SERIAL PRIMARY KEY,
    country_name VARCHAR(50) UNIQUE


)ENGINE=InnoDB;



CREATE TABLE covid_world_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    country_id BIGINT UNSIGNED,
    continent VARCHAR(20),
    submission_date TIMESTAMP NOT NULL,
    new_deaths INT,
    new_cases INT,
    total_cases BIGINT,
    total_deaths BIGINT,
    total_cases_per_million FLOAT,
    total_deaths_per_million FLOAT,
    new_cases_per_million FLOAT,
    new_deaths_per_million FLOAT,
    icu_patients INT,
    icu_patients_per_million FLOAT,
    hosp_patients INT,
    hosp_patients_per_million FLOAT,
    weekly_icu_admissions INT,
    weekly_icu_admissions_per_million FLOAT,
    weekly_hosp_admissions INT,
    weekly_hosp_admissions_per_million FLOAT,
    total_tests BIGINT,
    new_tests INT,
    total_tests_per_thousand FLOAT,
    new_tests_per_thousand FLOAT,
    tests_per_case FLOAT,
    positive_rate FLOAT,
    tests_units BIGINT,
    stringency_index FLOAT,
    population BIGINT,
    population_density FLOAT,
    median_age FLOAT,
    aged_65_older FLOAT,
    aged_70_older FLOAT,
    gdp_per_capita FLOAT,
    extreme_poverty FLOAT,
    cardiovasc_death_rate FLOAT,
    diabetes_prevalence FLOAT,
    handwashing_facilities FLOAT,
    hospital_beds_per_thousand FLOAT,
    life_expectancy FLOAT,
    human_development_index FLOAT,
    FOREIGN KEY (date_id) REFERENCES covid_date_dim(id),
    FOREIGN KEY (country_id) REFERENCES country_dim(id)
)ENGINE=InnoDB;
CREATE TABLE state_dim
(
    id SERIAL PRIMARY KEY,
    code VARCHAR(2),
    name VARCHAR(50)

)ENGINE=InnoDB;

CREATE TABLE covid_usa_fact
(
    id SERIAL PRIMARY KEY,
    date_id BIGINT UNSIGNED,
    state_id BIGINT UNSIGNED,
    submit_date TIMESTAMP NOT NULL,
    new_deaths INT,
    new_cases INT,
    total_cases INT,
    total_deaths INT,
    FOREIGN KEY (date_id) REFERENCES covid_date_dim(id),
    FOREIGN KEY (state_id) REFERENCES state_dim(id)
)ENGINE=InnoDB;


insert into state_dim (code,name) values ('AL','Alabama');
insert into state_dim (code,name) values ('AK','Alaska');
insert into state_dim (code,name) values ('AS','American Samoa');
insert into state_dim (code,name) values ('AZ','Arizona');
insert into state_dim (code,name) values ('AR','Arkansas');
insert into state_dim (code,name) values ('CA','California');
insert into state_dim (code,name) values ('CO','Colorado');
insert into state_dim (code,name) values ('CT','Connecticut');
insert into state_dim (code,name) values ('DE','Delaware');
insert into state_dim (code,name) values ('DC','District of Columbia');
insert into state_dim (code,name) values ('FM','Federated States of Micronesia');
insert into state_dim (code,name) values ('FL','Florida');
insert into state_dim (code,name) values ('GA','Georgia');
insert into state_dim (code,name) values ('GU','Guam');
insert into state_dim (code,name) values ('HI','Hawaii');
insert into state_dim (code,name) values ('ID','Idaho');
insert into state_dim (code,name) values ('IL','Illinois');
insert into state_dim (code,name) values ('IN','Indiana');
insert into state_dim (code,name) values ('IA','Iowa');
insert into state_dim (code,name) values ('KS','Kansas');
insert into state_dim (code,name) values ('KY','Kentucky');
insert into state_dim (code,name) values ('LA','Louisiana');
insert into state_dim (code,name) values ('ME','Maine');
insert into state_dim (code,name) values ('MH','Marshall Islands');
insert into state_dim (code,name) values ('MD','Maryland');
insert into state_dim (code,name) values ('MA','Massachusetts');
insert into state_dim (code,name) values ('MI','Michigan');
insert into state_dim (code,name) values ('MN','Minnesota');
insert into state_dim (code,name) values ('MS','Mississippi');
insert into state_dim (code,name) values ('MO','Missouri');
insert into state_dim (code,name) values ('MT','Montana');
insert into state_dim (code,name) values ('NE','Nebraska');
insert into state_dim (code,name) values ('NV','Nevada');
insert into state_dim (code,name) values ('NH','New Hampshire');
insert into state_dim (code,name) values ('NJ','New Jersey');
insert into state_dim (code,name) values ('NM','New Mexico');
insert into state_dim (code,name) values ('NY','New York');
insert into state_dim (code,name) values ('NC','North Carolina');
insert into state_dim (code,name) values ('ND','North Dakota');
insert into state_dim (code,name) values ('MP','Northern Mariana Islands');
insert into state_dim (code,name) values ('OH','Ohio');
insert into state_dim (code,name) values ('OK','Oklahoma');
insert into state_dim (code,name) values ('OR','Oregon');
insert into state_dim (code,name) values ('PW','Palau');
insert into state_dim (code,name) values ('PA','Pennsylvania');
insert into state_dim (code,name) values ('PR','Puerto Rico');
insert into state_dim (code,name) values ('RI','Rhode Island');
insert into state_dim (code,name) values ('SC','South Carolina');
insert into state_dim (code,name) values ('SD','South Dakota');
insert into state_dim (code,name) values ('TN','Tennessee');
insert into state_dim (code,name) values ('TX','Texas');
insert into state_dim (code,name) values ('UT','Utah');
insert into state_dim (code,name) values ('VT','Vermont');
insert into state_dim (code,name) values ('VI','Virgin Islands');
insert into state_dim (code,name) values ('VA','Virginia');
insert into state_dim (code,name) values ('WA','Washington');
insert into state_dim (code,name) values ('WV','West Virginia');
insert into state_dim (code,name) values ('WI','Wisconsin');
insert into state_dim (code,name) values ('WY','Wyoming');

/*countries*/

  
INSERT INTO country_dim (country_name) VALUES('Afghanistan');  
INSERT INTO country_dim (country_name) VALUES('Albania');  
INSERT INTO country_dim (country_name) VALUES('Algeria');  
INSERT INTO country_dim (country_name) VALUES('Andorra');  
INSERT INTO country_dim (country_name) VALUES('Angola');  
INSERT INTO country_dim (country_name) VALUES('Anguilla');  
INSERT INTO country_dim (country_name) VALUES('Antigua & Barbuda');  
INSERT INTO country_dim (country_name) VALUES('Argentina');  
INSERT INTO country_dim (country_name) VALUES('Armenia');  
INSERT INTO country_dim (country_name) VALUES('Australia');  
INSERT INTO country_dim (country_name) VALUES('Austria');  
INSERT INTO country_dim (country_name) VALUES('Azerbaijan');  
INSERT INTO country_dim (country_name) VALUES('Bahamas');  
INSERT INTO country_dim (country_name) VALUES('Bahrain');  
INSERT INTO country_dim (country_name) VALUES('Bangladesh');  
INSERT INTO country_dim (country_name) VALUES('Barbados');  
INSERT INTO country_dim (country_name) VALUES('Belarus');  
INSERT INTO country_dim (country_name) VALUES('Belgium');  
INSERT INTO country_dim (country_name) VALUES('Belize');  
INSERT INTO country_dim (country_name) VALUES('Benin');  
INSERT INTO country_dim (country_name) VALUES('Bermuda');  
INSERT INTO country_dim (country_name) VALUES('Bhutan');  
INSERT INTO country_dim (country_name) VALUES('Bolivia');  
INSERT INTO country_dim (country_name) VALUES('Bosnia & Herzegovina');  
INSERT INTO country_dim (country_name) VALUES('Botswana');  
INSERT INTO country_dim (country_name) VALUES('Brazil');  
INSERT INTO country_dim (country_name) VALUES('Brunei Darussalam');  
INSERT INTO country_dim (country_name) VALUES('Bulgaria');  
INSERT INTO country_dim (country_name) VALUES('Burkina Faso');  
INSERT INTO country_dim (country_name) VALUES('Myanmar/Burma');  
INSERT INTO country_dim (country_name) VALUES('Burundi');  
INSERT INTO country_dim (country_name) VALUES('Cambodia');  
INSERT INTO country_dim (country_name) VALUES('Cameroon');  
INSERT INTO country_dim (country_name) VALUES('Canada');  
INSERT INTO country_dim (country_name) VALUES('Cape Verde');  
INSERT INTO country_dim (country_name) VALUES('Cayman Islands');  
INSERT INTO country_dim (country_name) VALUES('Central African Republic');  
INSERT INTO country_dim (country_name) VALUES('Chad');  
INSERT INTO country_dim (country_name) VALUES('Chile');  
INSERT INTO country_dim (country_name) VALUES('China');  
INSERT INTO country_dim (country_name) VALUES('Colombia');  
INSERT INTO country_dim (country_name) VALUES('Comoros');  
INSERT INTO country_dim (country_name) VALUES('Congo');  
INSERT INTO country_dim (country_name) VALUES('Costa Rica');  
INSERT INTO country_dim (country_name) VALUES('Croatia');  
INSERT INTO country_dim (country_name) VALUES('Cuba');  
INSERT INTO country_dim (country_name) VALUES('Cyprus');  
INSERT INTO country_dim (country_name) VALUES('Czech Republic');  
INSERT INTO country_dim (country_name) VALUES('Democratic Republic of the Congo');  
INSERT INTO country_dim (country_name) VALUES('Denmark');  
INSERT INTO country_dim (country_name) VALUES('Djibouti');  
INSERT INTO country_dim (country_name) VALUES('Dominican Republic');  
INSERT INTO country_dim (country_name) VALUES('Dominica');  
INSERT INTO country_dim (country_name) VALUES('Ecuador');  
INSERT INTO country_dim (country_name) VALUES('Egypt');  
INSERT INTO country_dim (country_name) VALUES('El Salvador');  
INSERT INTO country_dim (country_name) VALUES('Equatorial Guinea');  
INSERT INTO country_dim (country_name) VALUES('Eritrea');  
INSERT INTO country_dim (country_name) VALUES('Estonia');  
INSERT INTO country_dim (country_name) VALUES('Ethiopia');  
INSERT INTO country_dim (country_name) VALUES('Fiji');  
INSERT INTO country_dim (country_name) VALUES('Finland');  
INSERT INTO country_dim (country_name) VALUES('France');  
INSERT INTO country_dim (country_name) VALUES('French Guiana');  
INSERT INTO country_dim (country_name) VALUES('Gabon');  
INSERT INTO country_dim (country_name) VALUES('Gambia');  
INSERT INTO country_dim (country_name) VALUES('Georgia');  
INSERT INTO country_dim (country_name) VALUES('Germany');  
INSERT INTO country_dim (country_name) VALUES('Ghana');  
INSERT INTO country_dim (country_name) VALUES('Great Britain');  
INSERT INTO country_dim (country_name) VALUES('Greece');  
INSERT INTO country_dim (country_name) VALUES('Grenada');  
INSERT INTO country_dim (country_name) VALUES('Guadeloupe');  
INSERT INTO country_dim (country_name) VALUES('Guatemala');  
INSERT INTO country_dim (country_name) VALUES('Guinea');  
INSERT INTO country_dim (country_name) VALUES('Guinea-Bissau');  
INSERT INTO country_dim (country_name) VALUES('Guyana');  
INSERT INTO country_dim (country_name) VALUES('Haiti');  
INSERT INTO country_dim (country_name) VALUES('Honduras');  
INSERT INTO country_dim (country_name) VALUES('Hungary');  
INSERT INTO country_dim (country_name) VALUES('Iceland');  
INSERT INTO country_dim (country_name) VALUES('India');  
INSERT INTO country_dim (country_name) VALUES('Indonesia');  
INSERT INTO country_dim (country_name) VALUES('Iran');  
INSERT INTO country_dim (country_name) VALUES('Iraq');  
INSERT INTO country_dim (country_name) VALUES('Israel and the Occupied Territories');  
INSERT INTO country_dim (country_name) VALUES('Italy');  
INSERT INTO country_dim (country_name) VALUES('Ivory Coast (Cote d''Ivoire)');  
INSERT INTO country_dim (country_name) VALUES('Jamaica');  
INSERT INTO country_dim (country_name) VALUES('Japan');  
INSERT INTO country_dim (country_name) VALUES('Jordan');  
INSERT INTO country_dim (country_name) VALUES('Kazakhstan');  
INSERT INTO country_dim (country_name) VALUES('Kenya');  
INSERT INTO country_dim (country_name) VALUES('Kosovo');  
INSERT INTO country_dim (country_name) VALUES('Kuwait');  
INSERT INTO country_dim (country_name) VALUES('Kyrgyz Republic (Kyrgyzstan)');  
INSERT INTO country_dim (country_name) VALUES('Laos');  
INSERT INTO country_dim (country_name) VALUES('Latvia');  
INSERT INTO country_dim (country_name) VALUES('Lebanon');  
INSERT INTO country_dim (country_name) VALUES('Lesotho');  
INSERT INTO country_dim (country_name) VALUES('Liberia');  
INSERT INTO country_dim (country_name) VALUES('Libya');  
INSERT INTO country_dim (country_name) VALUES('Liechtenstein');  
INSERT INTO country_dim (country_name) VALUES('Lithuania');  
INSERT INTO country_dim (country_name) VALUES('Luxembourg');  
INSERT INTO country_dim (country_name) VALUES('Republic of Macedonia');  
INSERT INTO country_dim (country_name) VALUES('Madagascar');  
INSERT INTO country_dim (country_name) VALUES('Malawi');  
INSERT INTO country_dim (country_name) VALUES('Malaysia');  
INSERT INTO country_dim (country_name) VALUES('Maldives');  
INSERT INTO country_dim (country_name) VALUES('Mali');  
INSERT INTO country_dim (country_name) VALUES('Malta');  
INSERT INTO country_dim (country_name) VALUES('Martinique');  
INSERT INTO country_dim (country_name) VALUES('Mauritania');  
INSERT INTO country_dim (country_name) VALUES('Mauritius');  
INSERT INTO country_dim (country_name) VALUES('Mayotte');  
INSERT INTO country_dim (country_name) VALUES('Mexico');  
INSERT INTO country_dim (country_name) VALUES('Moldova, Republic of');  
INSERT INTO country_dim (country_name) VALUES('Monaco');  
INSERT INTO country_dim (country_name) VALUES('Mongolia');  
INSERT INTO country_dim (country_name) VALUES('Montenegro');  
INSERT INTO country_dim (country_name) VALUES('Montserrat');  
INSERT INTO country_dim (country_name) VALUES('Morocco');  
INSERT INTO country_dim (country_name) VALUES('Mozambique');  
INSERT INTO country_dim (country_name) VALUES('Namibia');  
INSERT INTO country_dim (country_name) VALUES('Nepal');  
INSERT INTO country_dim (country_name) VALUES('Netherlands');  
INSERT INTO country_dim (country_name) VALUES('New Zealand');  
INSERT INTO country_dim (country_name) VALUES('Nicaragua');  
INSERT INTO country_dim (country_name) VALUES('Niger');  
INSERT INTO country_dim (country_name) VALUES('Nigeria');  
INSERT INTO country_dim (country_name) VALUES('Korea, Democratic Republic of (North Korea)');  
INSERT INTO country_dim (country_name) VALUES('Norway');  
INSERT INTO country_dim (country_name) VALUES('Oman');  
INSERT INTO country_dim (country_name) VALUES('Pacific Islands');  
INSERT INTO country_dim (country_name) VALUES('Pakistan');  
INSERT INTO country_dim (country_name) VALUES('Panama');  
INSERT INTO country_dim (country_name) VALUES('Papua New Guinea');  
INSERT INTO country_dim (country_name) VALUES('Paraguay');  
INSERT INTO country_dim (country_name) VALUES('Peru');  
INSERT INTO country_dim (country_name) VALUES('Philippines');  
INSERT INTO country_dim (country_name) VALUES('Poland');  
INSERT INTO country_dim (country_name) VALUES('Portugal');  
INSERT INTO country_dim (country_name) VALUES('Puerto Rico');  
INSERT INTO country_dim (country_name) VALUES('Qatar');  
INSERT INTO country_dim (country_name) VALUES('Reunion');  
INSERT INTO country_dim (country_name) VALUES('Romania');  
INSERT INTO country_dim (country_name) VALUES('Russian Federation');  
INSERT INTO country_dim (country_name) VALUES('Rwanda');  
INSERT INTO country_dim (country_name) VALUES('Saint Kitts and Nevis');  
INSERT INTO country_dim (country_name) VALUES('Saint Lucia');  
INSERT INTO country_dim (country_name) VALUES('Saint Vincent''s & Grenadines');  
INSERT INTO country_dim (country_name) VALUES('Samoa');  
INSERT INTO country_dim (country_name) VALUES('Sao Tome and Principe');  
INSERT INTO country_dim (country_name) VALUES('Saudi Arabia');  
INSERT INTO country_dim (country_name) VALUES('Senegal');  
INSERT INTO country_dim (country_name) VALUES('Serbia');  
INSERT INTO country_dim (country_name) VALUES('Seychelles');  
INSERT INTO country_dim (country_name) VALUES('Sierra Leone');  
INSERT INTO country_dim (country_name) VALUES('Singapore');  
INSERT INTO country_dim (country_name) VALUES('Slovak Republic (Slovakia)');  
INSERT INTO country_dim (country_name) VALUES('Slovenia');  
INSERT INTO country_dim (country_name) VALUES('Solomon Islands');  
INSERT INTO country_dim (country_name) VALUES('Somalia');  
INSERT INTO country_dim (country_name) VALUES('South Africa');  
INSERT INTO country_dim (country_name) VALUES('Korea, Republic of (South Korea)');  
INSERT INTO country_dim (country_name) VALUES('South Sudan');  
INSERT INTO country_dim (country_name) VALUES('Spain');  
INSERT INTO country_dim (country_name) VALUES('Sri Lanka');  
INSERT INTO country_dim (country_name) VALUES('Sudan');  
INSERT INTO country_dim (country_name) VALUES('Suriname');  
INSERT INTO country_dim (country_name) VALUES('Swaziland');  
INSERT INTO country_dim (country_name) VALUES('Sweden');  
INSERT INTO country_dim (country_name) VALUES('Switzerland');  
INSERT INTO country_dim (country_name) VALUES('Syria');  
INSERT INTO country_dim (country_name) VALUES('Tajikistan');  
INSERT INTO country_dim (country_name) VALUES('Tanzania');  
INSERT INTO country_dim (country_name) VALUES('Thailand');  
INSERT INTO country_dim (country_name) VALUES('Timor Leste');  
INSERT INTO country_dim (country_name) VALUES('Togo');  
INSERT INTO country_dim (country_name) VALUES('Trinidad & Tobago');  
INSERT INTO country_dim (country_name) VALUES('Tunisia');  
INSERT INTO country_dim (country_name) VALUES('Turkey');  
INSERT INTO country_dim (country_name) VALUES('Turkmenistan');  
INSERT INTO country_dim (country_name) VALUES('Turks & Caicos Islands');  
INSERT INTO country_dim (country_name) VALUES('Uganda');  
INSERT INTO country_dim (country_name) VALUES('Ukraine');  
INSERT INTO country_dim (country_name) VALUES('United Arab Emirates');  
INSERT INTO country_dim (country_name) VALUES('United States of America (USA)');  
INSERT INTO country_dim (country_name) VALUES('Uruguay');  
INSERT INTO country_dim (country_name) VALUES('Uzbekistan');  
INSERT INTO country_dim (country_name) VALUES('Venezuela');  
INSERT INTO country_dim (country_name) VALUES('Vietnam');  
INSERT INTO country_dim (country_name) VALUES('Virgin Islands (UK)');  
INSERT INTO country_dim (country_name) VALUES('Virgin Islands (US)');  
INSERT INTO country_dim (country_name) VALUES('Yemen');  
INSERT INTO country_dim (country_name) VALUES('Zambia');  
INSERT INTO country_dim (country_name) VALUES('Zimbabwe');