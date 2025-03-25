select *
from layoffs;

create table layoffs_edited
like layoffs;

select *
from layoffs_edited;

insert layoffs_edited
select *
from layoffs;

-- 1. remove duplicates
-- 2. standardize data
-- 3. null values or blank values
-- 4. remove any columns 


-- 1. REMOVING DUPLICATES 

select *,
row_number () over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) as row_num
from layoffs_edited;

with duplicate_cte as (
select *,
row_number () over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) as row_num
from layoffs_edited )
select *
from duplicate_cte
where row_num > 1;


CREATE TABLE `layoffs_edited2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_edited2;

insert layoffs_edited2
select *,
row_number () over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) as row_num
from layoffs_edited; 

delete
from layoffs_edited2
where row_num > 1;



-- 2. STANDARDIZING DATA
select *
from layoffs_edited2;

select company, trim(company)
from layoffs_edited2;

update layoffs_edited2
set company = trim(company);

select distinct industry
from layoffs_edited2
order by 1;

select *
from layoffs_edited2
where industry like 'cryp%';

update layoffs_edited2
set industry = 'crypto'
where industry like 'crypto%' ;

select distinct country
from layoffs_edited2
order by 1;

select distinct country, 
trim(trailing '.' from country)
from layoffs_edited2;

update layoffs_edited2
set country = trim(trailing '.' from country);

select *
from layoffs_edited2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_edited2;

update layoffs_edited2
set `date` =  str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_edited2
modify `date` date;

select *
from layoffs_edited2;

update layoffs_edited2
set industry = null
where industry = '';

select industry
from layoffs_edited2
order by 1;

select t1.industry, t2.industry
from layoffs_edited2 as t1
join layoffs_edited2 as t2
	on t1.company = t2.company
    and t1.location = t2.location
where t1.industry is null
and t2.industry is not null;

update layoffs_edited2 t1
join layoffs_edited2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_edited2
where industry is null;

select *
from layoffs_edited2
where total_laid_off is null 
and percentage_laid_off is null;

delete
from layoffs_edited2
where total_laid_off is null 
and percentage_laid_off is null;

select *
from layoffs_edited2;

alter table layoffs_edited2
drop column row_num;