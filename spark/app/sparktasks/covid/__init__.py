# import logging.config
#  from spark.app. sparktasks.util import PathUtil
# import os
#
#
# log_conf = os.path.join(PathUtil.get_empyreal_path(),'sparktasks/logging.conf')
# log_path = os.path.join(PathUtil.get_empyreal_path(),'sparktasks/logs','covid_economy.log')
#
# # Create logs folder if doesn't exist
# #log_path.parent.mkdir(parents=True, exist_ok=True)
#
# logging.config.fileConfig(fname=log_conf,
#                           defaults={'logfilename': log_path})