-- remove duplicates
create table layoffs_staging like layoffs;

insert into layoffs_staging select * from layoffs;

WITH duplicate_values_cte as (
select *, row_number() 
over(partition by company, location, industry, total_laid_off, `date`, country, funds_raised_millions) as duplicate_value
from layoffs_staging
)
select * from duplicate_values_cte where duplicate_value > 1;

select * from layoffs_staging where company = 'casper';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int(11) DEFAULT NULL,
  `duplicate_value` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2 select *, row_number() 
over(partition by company, location, industry, total_laid_off, `date`, country, funds_raised_millions) as duplicate_value
from layoffs_staging;

delete from layoffs_staging2 where duplicate_value > 1;

select * from layoffs_staging2 where duplicate_value > 1;

-- standarised data
update layoffs_staging2 set company = trim(company);

select distinct industry from layoffs_staging2 order by industry;

update layoffs_staging2 set industry = 'Crypto' where industry like 'Crypto%';

select * from layoffs_staging2 where industry = 'Crypto';

select distinct location from layoffs_staging2 order by location;

select distinct stage from layoffs_staging2 order by stage;

select distinct country from layoffs_staging2 order by country;

update layoffs_staging2 set country = trim(trailing '.' from country) where country like 'United States%';

select * from layoffs_staging2;

select `date`,
case
	when `date` like '%/%' then str_to_date(`date`, '%m/%d/%Y')
    when `date` like '%-%' then str_to_date(`date`, '%m-%d-%Y')
end as std_date
from layoffs_staging2;

update layoffs_staging2 set `date` = 
case
	when `date` like '%/%' then str_to_date(`date`, '%m/%d/%Y')
    when `date` like '%-%' then str_to_date(`date`, '%m-%d-%Y')
end;

alter table layoffs_staging2
modify column `date` date;

-- removing null or blanks
select t1.industry, t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = "")
and (t2.industry is not null and t2.industry != "");

update layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = "")
and (t2.industry is not null and t2.industry != "");

select * from 
layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null;

delete from 
layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null;

-- drop unnecessary columns
select * from layoffs_staging2;

alter table layoffs_staging2 drop column duplicate_value;