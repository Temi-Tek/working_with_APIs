-- 1. Dim Table for regions
CREATE TABLE carbon.dim_region (
    regionid INT PRIMARY KEY,
    shortname TEXT NOT NULL,
    dno TEXT
);

-- 2. Partitioned Intensity Table
CREATE TABLE carbon.fact_carbon_intensity (
    id SERIAL, 
    regionid INT REFERENCES carbon.dim_region(regionid),
    date_recorded DATE NOT NULL,
    intensity FLOAT,
    index TEXT,
    -- Composite Unique Key: Ensures 1 record per region per day
    PRIMARY KEY (regionid, date_recorded) 
) PARTITION BY RANGE (date_recorded);

-- Create a partition for the year 2026
CREATE TABLE carbon.fact_carbon_intensity_2026 PARTITION OF carbon.fact_carbon_intensity
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
-- 3. Partitioned Generation Mix Table
CREATE TABLE carbon.fact_generation_mix (
    id SERIAL,
    regionid INT REFERENCES carbon.dim_region(regionid),
    date_recorded DATE NOT NULL,
    biomass FLOAT, coal FLOAT, imports FLOAT,
    gas FLOAT, nuclear FLOAT, other FLOAT,
    hydro FLOAT, solar FLOAT, wind FLOAT,
    PRIMARY KEY (regionid, date_recorded)
) PARTITION BY RANGE (date_recorded);

-- Create a partition for the year 2026
CREATE TABLE carbon.fact_generation_mix_2026 PARTITION OF carbon.fact_generation_mix
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');