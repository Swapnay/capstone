SELECT * FROM (
 SELECT  c.country_name AS country,d.month AS month,d.year AS year,avg(cd.new_cases_per_million) AS avg_cases,cod.extreme_poverty,cod.human_development_index,
cd.new_tests,cd.new_tests_per_thousand, rank()  over(partition BY d.month,d.year ORDER BY AVG(cd.new_cases_per_million) ) AS ranking FROM covid_economy_impact.covid_world_normalized_fact cd
 LEFT JOIN covid_economy_impact.country_dim c ON cd.country_id = c.id
 JOIN country_details_dim cod on cod.country_id = cd.country_id
 JOIN covid_economy_impact.covid_date_dim d on d.id = cd.date_id AND (m.month =3 or m.month=10) GROUP BY country,month) ranked
 where ranking<=10;