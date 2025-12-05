CREATE TABLE users (
    user_id BIGINT PRIMARY KEY
);

CREATE TABLE wines (
    wine_id INTEGER PRIMARY KEY,
    wine_name VARCHAR NOT NULL,
    type VARCHAR,
    elaborate VARCHAR,
    grapes VARCHAR,
    harmonize VARCHAR,
    abv DOUBLE,
    body VARCHAR,
    acidity VARCHAR,
    code VARCHAR,
    country VARCHAR,
    region_id INTEGER,
    region_name VARCHAR,
    winery_id INTEGER,
    winery_name VARCHAR,
    website VARCHAR,
    vintages VARCHAR
);

CREATE TABLE ratings (
    rating_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    wine_id INTEGER NOT NULL,
    vintage VARCHAR,
    rating DOUBLE NOT NULL CHECK (rating IN (1.0,1.5,2.0,2.5,3.0,3.5,4.0,4.5,5.0)),
    rating_date TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (wine_id) REFERENCES wines(wine_id)
);
