CREATE TABLE dim_date (
    pk_date_id INT PRIMARY KEY,
    date DATE,
    day TINYINT,
    day_suffix CHAR(2) NOT NULL,
    weekday TINYINT NOT NULL,
    weekday_name VARCHAR(10) NOT NULL,
    month TINYINT NOT NULL,
    monthname VARCHAR(10) NOT NULL,
    quarter TINYINT NOT NULL,
    quartername VARCHAR(6) NOT NULL,
    year INT NOT NULL,
    monthyear CHAR(7) NOT NULL,
    isweekend VARCHAR(MAX) NOT NULL,
    isholiday VARCHAR(MAX) NOT NULL
);