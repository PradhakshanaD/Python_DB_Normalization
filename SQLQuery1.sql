select * 
from Portfolio_Project1..CovidDeaths 
order by 3,4

--select * 
--from Portfolio_Project1..CovidVaccinations 
--order by 3,4

--selecting specific fields that seem important for better understanding
select location, date, total_cases, new_cases, total_deaths, population 
from Portfolio_Project1..CovidDeaths 
order by 1,2

--creating a column by assessing the number of deaths w.r.t total cases
--also showing the likelihood of death in specific countries when contracted by the disease
select location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage 
from Portfolio_Project1..CovidDeaths 
where location like 'Ind%'
order by 1,2

--showing the percentage of population that got affected by the disease
select location, date, population, total_cases,  (total_cases/population)*100 as CovidPercentage 
from Portfolio_Project1..CovidDeaths 
order by 1,2

--looking at countries with highest infection rate
select location, population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as CovidPercentage 
from Portfolio_Project1..CovidDeaths 
Group by location, population
order by CovidPercentage desc

--looking at countries with highest death count
select location, MAX(cast(total_deaths as int)) as DeathCount
from Portfolio_Project1..CovidDeaths 
where continent is not null
Group by location
order by DeathCount desc

--breaking things by continent. showing continents with highest death count per population
select location, MAX(cast(total_deaths as int)) as DeathCount
from Portfolio_Project1..CovidDeaths 
where continent is null
Group by location
order by DeathCount desc

--Global numbers (cases and deaths across the world per day)
select date, sum(new_cases) as total_cases, sum(cast(new_deaths as int))as total_deaths, sum(cast(new_deaths as int))/sum(new_cases) *100 as DeathPercentage 
from Portfolio_Project1..CovidDeaths 
where continent is not null
group by date
order by 1,2

--Global numbers (cases and deaths across the world in total)
select sum(new_cases) as total_cases, sum(cast(new_deaths as int))as total_deaths, sum(cast(new_deaths as int))/sum(new_cases) *100 as DeathPercentage 
from Portfolio_Project1..CovidDeaths 
where continent is not null
--group by date
order by 1,2

--joining both the tables and looking at new-vaccinations per day
select d.continent, d.location, d.date, d.population, v.new_vaccinations
from Portfolio_Project1..CovidDeaths as d
join Portfolio_Project1..CovidVaccinations as v
on d.location = v.location
and d.date = v.date
where d.continent is not null
order by 1,2,3

--summing the vaccinations per day by location to have a total count of the vaccinations done
select d.continent, d.location, d.date, d.population, v.new_vaccinations,
sum(convert(int,v.new_vaccinations)) over (partition by (d.location) order by d.location, d.date) as Rollingvaccination_by_location
from Portfolio_Project1..CovidDeaths as d
join Portfolio_Project1..CovidVaccinations as v
on d.location = v.location
and d.date = v.date
where d.continent is not null
order by 2,3

--CTE or 
with populationvsvaccination (continent,location,date,population,new_vaccinations,Rollingvaccination_by_location)
as
(
select d.continent, d.location, d.date, d.population, v.new_vaccinations,
sum(convert(int,v.new_vaccinations)) over (partition by (d.location) order by d.location, d.date) as Rollingvaccination_by_location
from Portfolio_Project1..CovidDeaths as d
join Portfolio_Project1..CovidVaccinations as v
on d.location = v.location
and d.date = v.date
where d.continent is not null
--order by 2,3
)
select *, (Rollingvaccination_by_location/population) *100 as Vaccination_Percentage
from populationvsvaccination

--creating a temporary table
drop table if exists #PopvsVac
create table #PopvsVac
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
Rollingvaccination_by_location numeric,
)

Insert into #PopvsVac
select d.continent, d.location, d.date, d.population, v.new_vaccinations,
sum(convert(int,v.new_vaccinations)) over (partition by (d.location) order by d.location, d.date) as Rollingvaccination_by_location
from Portfolio_Project1..CovidDeaths as d
join Portfolio_Project1..CovidVaccinations as v
on d.location = v.location
and d.date = v.date
where d.continent is not null
--order by 2,3

select *, (Rollingvaccination_by_location/population) *100 as Vaccination_Percentage
from #PopvsVac

--creating a view
--joining both the tables and looking at new-vaccinations per day
Create View vaccinationsperday as
select d.continent, d.location, d.date, d.population, v.new_vaccinations,
sum(convert(int,v.new_vaccinations)) over (partition by (d.location) order by d.location, d.date) as Rollingvaccination_by_location
from Portfolio_Project1..CovidDeaths as d
join Portfolio_Project1..CovidVaccinations as v
on d.location = v.location
and d.date = v.date
where d.continent is not null
--order by 2,3
--Go

--query from the created view
select *
from vaccinationsperday

