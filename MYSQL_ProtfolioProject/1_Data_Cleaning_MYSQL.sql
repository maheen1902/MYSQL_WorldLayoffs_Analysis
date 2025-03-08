-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT 
    *
FROM
    layoffs_staging;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary



-- 1. Remove Duplicates

WITH Duplicate_entites As(
SELECT 
*,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) As Rownum
FROM layoffs_staging)

-- Checking for duplicates
SELECT *
FROM layoffs_staging2
WHERE Rownum > 1;

-- confirming the findings ; Yahoo had 2 dubplicates
SELECT * FROM layoffs_staging 
WHERE company = 'Yahoo'

-- Creating another table to delete the dubplicates

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);


INSERT into layoffs_staging2
SELECT 
*,
ROW_NUMBER()
 OVER
 (PARTITION BY 
    company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) As Rownum
FROM layoffs_staging;

-- Deleting the duplicates
delete 
FROM layoffs_staging2
WHERE
    row_num > 1;

-- confirming the DELETE command
SELECT * FROM layoffs_staging2 
WHERE company = 'Yahoo' -- Got only one row on Yahoo now

-- Layoffs_staging2 Table
SELECT 
    *
FROM
    layoffs_staging2;


-- 2. Standarizing the Data

-- Checking for white spaces
SELECT 
    Company, TRIM(company)
FROM   
    layoffs_staging2
WHERE
    Company <> TRIM(company);

-- Trimming the White spaces
Update layoffs_staging2
Set company = TRIM(company);

SELECT Distinct 
    industry
from 
    layoffs_staging2
ORDER BY
    1; 

-- inustry had 2 variances for crypto (crypto,crypto Currency)
SELECT * 
FROM
    layoffs_staging2
WHERE
    industry LIKE 'crypto%';

-- Standarizng crypto on all rows
Update layoffs_staging2
Set
    industry = 'Crypto'
WHERE
    industry LIKE 'crypto%';

-- Standarizing location column

SELECT Distinct 
    location
from 
    layoffs_staging2
ORDER BY
    1;  -- no changes requried 


-- Standarizing countries

SELECT Distinct 
    country
from 
    layoffs_staging2
ORDER BY
    1; 

-- Fixing United States. 
Update layoffs_staging2 
SET country = TRIM(Trailing '.' from country)
WHERE   
    country Like 'United States%'

-- Standarizing date ; changing from Type TEXT to DATE format

Update layoffs_staging2
SET date = STR_TO_DATE(`date`,'%m/%d/%Y')

-- modifying DATE TYPE  

ALTER TABLE layoffs_staging2
MODIFY column `date` DATE;

-- 3. Looking at null values and blanks

SELECT *
from
    layoffs_staging2
WHERE
    percentage_laid_off is null
and
    total_laid_off is null; -- useless rows

DELETE
FROM   
    layoffs_staging2
WHERE
    percentage_laid_off is null
and
    total_laid_off is null;


SELECT * 
from
    layoffs_staging2
WHERE
    industry is null
    OR
    industry = '';  --can fix these by looking at other rows

DELETE
FROM   
    layoffs_staging2

SELECT *
from
    Layoffs_staging2
WHERE
    company = 'Airbnb'; -- Airbnb has tarvel industry in one row


SELECT
    t1.company,
    t1.industry,
    t2.industry
FROM    
    layoffs_staging2 t1
join
    layoffs_staging2 t2
on
    t1.company = t2.company
    and
    t1.location = t2.location
WHERE
    (t1.industry is null or t1.industry = '')
    and
    t2.industry is not null;

-- Updating industries 

Update 
    layoffs_staging2
SET 
    industry = null
WHERE
    industry = '';

Update 
    layoffs_staging2 t1
join
    layoffs_staging2 t2
on
    t1.company = t2.company
    and
    t1.location = t2.location
SET 
    t1.industry = t2.industry
WHERE
    (t1.industry is null)
    and
    t2.industry is not null;

-- Bally's still has no industry as its only had one row
SELECT *
from
    Layoffs_staging2
WHERE
    company Like 'Bally%'; 


-- 4. removing any columns and rows that are not necessary

-- row_num is not required anymore

ALTER TABLE layoffs_staging2
Drop column row_num;