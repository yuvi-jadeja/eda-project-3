-- exploratory data analysis
select * from layoffs_staging2;

select max(total_laid_off) as max_total_laid_off, 
max(percentage_laid_off) as max_percentage_laid_off 
from layoffs_staging2;

select * from layoffs_staging2
order by total_laid_off desc;

select company, location, industry, total_laid_off, percentage_laid_off, stage, funds_raised_millions from layoffs_staging2
where country = "India"
order by total_laid_off desc;

select company, sum(total_laid_off) as sum_of_total_laid_off, sum(percentage_laid_off)
from layoffs_staging2
group by company 
order by sum_of_total_laid_off desc;

select min(`date`) as layoffs_start, max(`date`) layoffs_end, sum(total_laid_off) as total_laid_off
from layoffs_staging2;

select industry, sum(total_laid_off) as sum_of_total_laid_off
from layoffs_staging2
group by industry
order by sum_of_total_laid_off desc;

select country, sum(total_laid_off) as sum_of_total_laid_off
from layoffs_staging2
group by country
order by sum_of_total_laid_off desc;

select year(`date`) as year_wise, sum(total_laid_off) 
from layoffs_staging2
group by year_wise
order by year_wise desc;

select substring(`date`, 1, 7) as month_wise, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by month_wise
having month_wise is not null
order by month_wise;

with rolling_total as(
select substring(`date`, 1, 7) as month_wise, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by month_wise
having month_wise is not null
order by month_wise
)
select month_wise, total_laid_off, 
sum(total_laid_off) over(order by month_wise) as rolling_total from rolling_total;

with year_wise_highest_company as (
select company, year(`date`) as year_wise, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, year_wise
order by year_wise, total_laid_off desc
), company_ranking_year_wise as(
select *, dense_rank() over(partition by year_wise order by total_laid_off desc) as ranking
from year_wise_highest_company
where year_wise is not null
)
select * from company_ranking_year_wise
where ranking <= 3;

with year_wise_highest_industry as (
select industry, year(`date`) as year_wise, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by industry, year_wise
order by year_wise, total_laid_off desc
), industry_ranking_year_wise as(
select *, dense_rank() over(partition by year_wise order by total_laid_off desc) as ranking
from year_wise_highest_industry
where year_wise is not null
)
select * from industry_ranking_year_wise
where ranking <= 3;