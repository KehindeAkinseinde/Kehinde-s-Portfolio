-- Exploratory data analysis

select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- when the layoffs started and when they stopped
select min(`date`), max(`date`)
from layoffs_staging2;

-- what hit industry got the most layoffs
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

-- which country had the most  layoffs
select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

-- we can look at the layofffs by year
select year (`date`), sum(total_laid_off)
from layoffs_staging2
group by year( `date`)
order by 1 desc;

-- show the stage of the company, shows different series they are in
select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

-- show percentage of layoffs
select company, sum(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- progression of layoff
-- rolling total
select substring(`date`,1,7) as `MONTH`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) IS NOT NULL
group by `MONTH`
order by 1 ASC;

-- Rolling sum : when you want to take the data from above and want to get the 
-- rolling sum based of the data do a cte

with Rolling_Total as
(
select substring(`date`,1,7) as `MONTH`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) IS NOT NULL
group by `MONTH`
order by 1 ASC
)
select `MONTH`, total_off,
sum(total_off) over(order by `MONTH`) as rolling_total
from Rolling_Total ;

-- How much comopany laid off per year
select company, YEAR( `date`), sum(total_laid_off)
from layoffs_staging2
group by company, YEAR(`date`)
order by company asc;

-- rank each year they laid off the most of the empoloyees
select company, YEAR( `date`), sum(total_laid_off)
from layoffs_staging2
group by company, YEAR(`date`)
order by 3 desc;

with Company_Year (company, years, total_laid_off) AS 
(
select company, YEAR( `date`), sum(total_laid_off)
from layoffs_staging2
group by company, YEAR(`date`)
), 
-- filter the ranking to only show top 5 companies per year
Company_Year_Rank AS (
-- partition by year and rank by total laid off
select *,
dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from Company_Year
-- to take out the null values
where years is not null
)
select *
from Company_Year_Rank
where ranking <= 5;
