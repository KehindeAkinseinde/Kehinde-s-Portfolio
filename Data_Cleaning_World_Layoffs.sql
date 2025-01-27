 -- First project
 -- Data Cleaniong
 -- Get data in a more usable format, so you fix a lot of 
 -- issues in the raw data so that when you start using 
 -- visualizations and using it in you products the data is actually useful

SELECT * 
FROM world_layoffs.layoffs;
-- Steps:
 -- 1. remove duplicates
 -- 2. standardize the data: if there issues with the spelling or something the data we want to fix it and standardize it
 -- NULL VALUES OR BLANK VALUES
 -- Remove unnecessay columns
 
 

-- first thing we want to do is create a staging table. This is the one we will work in 
-- and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

SELECT *
FROM world_layoffs.layoff_staging;

INSERT layoffs_staging 
SELECT * 
FROM world_layoffs.layoffs;

SELECT *,
ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
	
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT *
FROM duplicate_cte
where row_num > 1;

-- To confirm these companies are duplicates
-- SELECT *
-- FROM layoffs_staging
-- WHERE company = 'casper';


-- one solution, which I think is a good one. Is to create a new column and add those row 
-- numbers in. Then delete where row numbers are over 1, then delete that column

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

 SELECT *
 FROM layoffs_staging2
 WHERE row_num > 1;
 
 INSERT INTO layoffs_staging2
 SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 1
  DELETE
 FROM layoffs_staging2
 WHERE row_num > 1;
 
 SELECT *
 FROM layoffs_staging2;

-- Standardizing data

select distinct (company), trim(company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

-- I also noticed the Crypto has multiple different variations.
-- We need to standardize that - let's say all to Crypto
select  distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto';

-- we also need to look at 
-- everything looks good except apparently we have some 
-- "United States" and some "United States." with a period at the end. Let's standardize this.
select distinct country
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united states%';

-- now if we run this again it is fixed
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

-- Let's also fix the date columns:
select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2; 
-- we can use str to date to update this field
UPDATE layoffs_staging2
set `date` = STR_TO_DATE(`dat
e`, '%m/%d/%Y');

-- now we can convert the data type properly
alter table layoffs_staging2
modify column `date` DATE;

select `date`
from layoffs_staging2; 

select *
from layoffs_staging2; 

-- Working with Nulls and blank values
-- if we look at industry it looks like we have some null and empty rows, let's take a look 
-- at these

select *
from layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2 
where industry is null
or industry = '';

-- let's take a look at these
select *
from layoffs_staging2 
where company like 'bally%';

-- nothing here
select *
from layoffs_staging2
where company = 'Airbnb';

 -- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it 
-- to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all


select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set  t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

-- deleting data must be very confident thing to do
-- Delete Useless data we can't really use
select *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

select *
from layoffs_staging2;

-- No longer need row_num colomn 
alter table layoffs_staging2
drop column row_num;