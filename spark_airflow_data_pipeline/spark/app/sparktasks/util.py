import pathlib
import os


class PathUtil:

    @staticmethod
    def get_empyreal_path():
        print(os.path.dirname(os.path.abspath(__file__)))
        return pathlib.Path(__file__).parents[1]