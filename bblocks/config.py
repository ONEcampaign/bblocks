import os


class Paths:
    def __init__(self, project_dir):
        self.project_dir = project_dir

    @property
    def imported_data(self):
        return os.path.join(self.project_dir, "bblocks", "import_tools", "stored_data")

    @property
    def test_files(self):
        return os.path.join(self.project_dir, "tests", "test_files")
    
    @property
    def import_tools(self):
        return os.path.join(self.project_dir, "bblocks", "import_tools")


PATHS = Paths(os.path.dirname(os.path.dirname(__file__)))
