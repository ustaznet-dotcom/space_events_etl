-- mysql/init.sql
CREATE TABLE IF NOT EXISTS space_categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100),
    description TEXT
);

INSERT INTO space_categories (category_id, category_name) VALUES
(1, 'Space Launch'),
(2, 'Space Exploration'),
(3, 'Astronomy Event'),
(4, 'Conference'),
(5, 'Anniversary');

CREATE TABLE IF NOT EXISTS event_sources (
    source_id INT PRIMARY KEY,
    source_name VARCHAR(100),
    url VARCHAR(255)
);

CREATE TABLE events (
    event_id INT,
    name VARCHAR(255),
    source_id INT,  -- ← ссылка на источник
    FOREIGN KEY (source_id) REFERENCES event_sources(source_id)
);