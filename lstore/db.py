from lstore.table import Table
"""
The Database class is a general interface to the database and handles high-level operations such as
starting and shutting down the database instance and loading the database from stored disk files.
This class also handles the creation and deletion of tables via the create and drop function. The
create function will create a new table in the database. The Table constructor takes as input the
name of the table, the number of columns, and the index of the key column. The drop function
drops the specified table.
"""
import os
"""
Source: https://www.freecodecamp.org/news/creating-a-directory-in-python-how-to-create-a-folder/
"""
class Database():

    def __init__(self):
        self.tables = []
        self.folder_path = ""
        pass

    def open(self, path):
        if os.path.exists(path):
            pass
        else:
            os.mkdir(path)
            self.folder_path = path

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i in range(len(self.tables)):
            if self.tables[i].name == name:
                self.tables.pop(i)
        return
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        return None 
