'''import logging.config
 from  sparktasks.util import  PathUtil

log_conf = PathUtil.get_empyreal_path().joinpath('sparktasks/logging.conf')
log_path = PathUtil.get_empyreal_path().joinpath('logs').joinpath('covid_economy.log')


# Create logs folder if doesn't exist
log_path.parent.mkdir(parents=True, exist_ok=True)

logging.config.fileConfig(fname=log_conf,
                          defaults={'logfilename': log_path},
                          disable_existing_loggers= False)'''