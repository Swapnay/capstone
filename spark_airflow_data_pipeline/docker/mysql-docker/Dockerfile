FROM mysql:8.0.19

ADD mysql-initdb.d/covid_economy_impact.sql /docker-entrypoint-initdb.d/
RUN chmod -R +x /docker-entrypoint-initdb.d