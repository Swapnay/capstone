USE covid_economy_impact;
DROP TABLE IF EXISTS housing_date_dim;
CREATE TABLE housing_date_dim
(
    id SERIAL PRIMARY KEY,
    month TINYINT,
    year SMALLINT,
    UNIQUE(month,year)
)ENGINE=InnoDB;

DROP TABLE IF EXISTS city_dim;
CREATE TABLE city_dim
(
     id SERIAL PRIMARY KEY,
     city_name VARCHAR(50) UNIQUE
)ENGINE=InnoDB;

DROP TABLE IF EXISTS metro_dim;
CREATE TABLE metro_dim
(
    id SERIAL PRIMARY KEY,
    metro_city_name VARCHAR(50) UNIQUE

)ENGINE=InnoDB;

DROP TABLE IF EXISTS county_dim;
CREATE TABLE county_dim
(
     id SERIAL PRIMARY KEY,
     county_name VARCHAR(50) UNIQUE
)ENGINE=InnoDB;

CREATE TABLE home_prices_fact
(
    id SERIAL PRIMARY KEY,
    city_id BIGINT UNSIGNED,
    metro_id BIGINT UNSIGNED,
    date_id BIGINT UNSIGNED,
    inventory_date timestamp  NOT NULL,
    state_id BIGINT UNSIGNED,
    county_id BIGINT UNSIGNED,
    mid_tier DECIMAL(13,2) DEFAULT 0.00,
    top_tier DECIMAL(13,2) DEFAULT 0.00,
    bottom_tier DECIMAL(13,2) DEFAULT 0.00,
    single_family DECIMAL(13,2) DEFAULT 0.00,
    condo DECIMAL(13,2) DEFAULT 0.00,
    1bd_room DECIMAL(13,2) DEFAULT 0.00,
    2db_room DECIMAL(13,2) DEFAULT 0.00,
    3bd_room DECIMAL(13,2) DEFAULT 0.00,
    4bd_room DECIMAL(13,2) DEFAULT 0.00,
    5bd_room DECIMAL(13,2) DEFAULT 0.00,
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id),
    FOREIGN KEY (city_id) REFERENCES city_dim(id),
    FOREIGN KEY (metro_id) REFERENCES metro_dim(id),
    FOREIGN KEY (state_id) REFERENCES state_dim(id),
    FOREIGN KEY (county_id) REFERENCES county_dim(id),
    UNIQUE(city_id, metro_id, date_id, state_id, county_id)
)ENGINE=InnoDB;

CREATE TABLE home_prices_fact
(
    id SERIAL PRIMARY KEY,
    city_id BIGINT UNSIGNED,
    metro_id BIGINT UNSIGNED,
    date_id BIGINT UNSIGNED,
    inventory_date timestamp ,
    state_id BIGINT UNSIGNED,
    county_id BIGINT UNSIGNED,
    inventory_type ENUM('mid_tier', 'top_tier','bottom_tier', 'single_family',
    'condo','1bd','2bd','3bd','4bd','5bd'),
    price DECIMAL(13,2) DEFAULT 0.00,
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id),
    FOREIGN KEY (city_id) REFERENCES city_dim(id),
    FOREIGN KEY (metro_id) REFERENCES metro_dim(id),
    FOREIGN KEY (state_id) REFERENCES state_dim(id),
    FOREIGN KEY (county_id) REFERENCES county_dim(id),
    UNIQUE(city_id, metro_id, date_id, state_id, county_id,inventory_type)
)ENGINE=InnoDB;

CREATE TABLE home_inventory_sales_fact
(
    id SERIAL PRIMARY KEY,
    metro_id BIGINT UNSIGNED,
    date_id BIGINT UNSIGNED,
    inventory_date timestamp NOT NULL,
    state_id BIGINT UNSIGNED,
    for_sale INT,
    median_days_to_sale_pending TINYINT,
    median_list_price DECIMAL(13,2),
    median_sale_price DECIMAL(13,2),
    FOREIGN KEY (date_id) REFERENCES housing_date_dim(id),
    FOREIGN KEY (metro_id) REFERENCES metro_dim(id),
    FOREIGN KEY (state_id) REFERENCES state_dim(id),
    UNIQUE( metro_id, date_id, state_id)
)ENGINE=InnoDB;

CREATE TABLE home_prices_monthly
(
    id SERIAL PRIMARY KEY,
    state VARCHAR(10) NOT NULL,
    state_name VARCHAR(50) NOT NULL,
    inventory_type ENUM('mid_tier', 'top_tier','bottom_tier', 'single_family',
    'condo','1bd','2bd','3bd','4bd','5bd'),
    avg_price DECIMAL(13,2) DEFAULT 0.00,
    year INT NOT NULL,
    month TiNYINT,
    inventory_date Date,
    UNIQUE(inventory_type,state, month,year)
)ENGINE=InnoDB;

CREATE TABLE home_inventory_monthly
(
    id SERIAL PRIMARY KEY,
    state VARCHAR(10) NOT NULL,
    state_name VARCHAR(50) NOT NULL,
    inventory_type enum('days_to_pending','for_sale','median_sale_price','median_list_price','mean_price_cut','median_price_cut'),
    days DECIMAL(13,2) DEFAULT 0.00,
    year INT NOT NULL,
    month TiNYINT,
    inventory_date DATE,
    UNIQUE(inventory_type,state, month,year)
)ENGINE=InnoDB;







