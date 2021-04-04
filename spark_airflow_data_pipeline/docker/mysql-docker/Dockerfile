FROM mysql:8.0

ADD covid_economy_impact.sql /docker-entrypoint-initdb.d/
RUN chmod -R +x /docker-entrypoint-initdb.d