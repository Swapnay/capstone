import logging.config


log_conf = "/usr/local/spark/app/sparktasks/logging.conf"
#log_path = PathUtil.get_empyreal_path().joinpath('sparktasks/').joinpath('covid_economy.log')

# Create logs folder if doesn't exist
#log_path.parent.mkdir(parents=True, exist_ok=True)

logging.config.fileConfig(fname=log_conf,
                          defaults={'logfilename': '/usr/local/spark/app/sparktasks/logs/covid_economy.log'})