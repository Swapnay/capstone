import pathlib
import os


class PathUtil:

    @staticmethod
    def get_empyreal_path():
        return os.path.join("usr", "local", "spark", "app")#os.path.join(os.getenv('SPARK_HOME'),"app")#"