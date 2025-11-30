from lstore.table import Table
from lstore.bufferpool import BufferPool
from lstore.lock_manager import LockManager
import os
import json

"""
The Database class is a general interface to the database and handles high-level operations such as
starting and shutting down the database instance and loading the database from stored disk files.
This class also handles the creation and deletion of tables via the create and drop function. The
create function will create a new table in the database. The Table constructor takes as input the
name of the table, the number of columns, and the index of the key column. The drop function
drops the specified table.
"""
"""
Source: https://www.freecodecamp.org/news/creating-a-directory-in-python-how-to-create-a-folder/
"""
class Database():

    def __init__(self):
        self.tables = []
        self._tables_by_name = {}
        self._path = None
        #bufferpool handle (created in open)
        self.bufferpool = None
        #lock manager handle (created in open)
        self.lock_manager = None

    # Milestone 2: simple JSON-based persistence
    def open(self, path):
        """
        Initialize database storage at `path` and load existing tables if present.
        """
        self._path = path
        os.makedirs(self._path, exist_ok=True)

        # set up bufferpool pages directory and instance
        pages_dir = os.path.join(self._path, "pages")
        os.makedirs(pages_dir, exist_ok=True)

        # you can change capacity to what ever 
        self.bufferpool = BufferPool(capacity=256, root_dir=pages_dir)
        
        # set up lock manager for 2PL concurrency control
        self.lock_manager = LockManager()

        catalog_path = os.path.join(self._path, 'catalog.json')
        self.tables = []
        self._tables_by_name = {}

        if not os.path.exists(catalog_path):
            # fresh DB
            with open(catalog_path, 'w') as f:
                json.dump({"tables": []}, f)
            return

        try:
            with open(catalog_path, 'r') as f:
                catalog = json.load(f)
        except Exception:
            catalog = {"tables": []}

        for tmeta in catalog.get("tables", []):
            name = tmeta.get("name")
            table_file = os.path.join(self._path, f'{name}.json')
            if not os.path.exists(table_file):
                continue
            try:
                with open(table_file, 'r') as tf:
                    data = json.load(tf)
                table = Table(data["name"], int(data["num_columns"]), int(data["key"]), self.bufferpool)

                # restore counters
                table._next_base_rid = int(data.get("next_base_rid", 1))
                table._next_tail_rid = int(data.get("next_tail_rid", 1000000000))

                # Table Rows -> (list of [rid, row])
                rows_list = data.get("rows", [])
                for rid, row in rows_list:
                    table._rows[int(rid)] = list(row)

                # rebuild head pointers 
                for rid, row in table._rows.items():
                    if rid < 1_000_000_000:  
                        table._head[rid] = row[0]  

                # restore pk mapping if present; 
                # or remake
                pk_list = data.get("pk", None)
                if pk_list is not None:
                    for k, br in pk_list:
                        table._pk[int(k)] = int(br)
                else:
                    for rid, row in table._rows.items():
                        if rid < 1000000000:
                            key_val = row[4 + table.key]
                            table._pk[int(key_val)] = rid

                # restore deleted flags
                del_list = data.get("deleted", [])
                for br, flag in del_list:
                    table._deleted[int(br)] = bool(flag)

                # rebuild primary key index structure if present
                try:
                    for k, br in table._pk.items():
                        table._index_add_pk(k, br)
                except Exception:
                    pass

                self.tables.append(table)
                self._tables_by_name[table.name] = table
            except Exception:
                # skip corrupted table file
                continue

    def close(self):
        """
        Persist all tables to disk if `open` has been called with a path.
        """
        if not self._path:
            return

        catalog = {"tables": []}
        for table in self.tables:
            catalog["tables"].append({
                "name": table.name,
                "num_columns": table.num_columns,
                "key": table.key,
            })

            table_path = os.path.join(self._path, f'{table.name}.json')
            data = {
                "name": table.name,
                "num_columns": table.num_columns,
                "key": table.key,
                "next_base_rid": table._next_base_rid,
                "next_tail_rid": table._next_tail_rid,
                # store as list of pairs to avoid JSON dict key coercion
                "rows": [[int(rid), row] for rid, row in table._rows.items()],
                "pk": [[int(k), int(br)] for k, br in table._pk.items()],
                "deleted": [[int(br), bool(flag)] for br, flag in table._deleted.items()],
            }
            try:
                with open(table_path, 'w') as tf:
                    json.dump(data, tf)
            except Exception:
                # ignore persistence errors for individual tables
                pass

        try:
            with open(os.path.join(self._path, 'catalog.json'), 'w') as f:
                json.dump(catalog, f)
        except Exception:
            pass

        # lush any dirty pages from the bufferpool to disk
        if self.bufferpool is not None:
            self.bufferpool.persist_all()

    """
    Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # For M2 part1 semantics, ensure a fresh table when create_table is called
        # even if one with the same name was loaded from disk. Part2 will use get_table.
        if name in self._tables_by_name:
            # drop the existing in-memory table with same name
            self.tables = [t for t in self.tables if t.name != name]
            del self._tables_by_name[name]
        table = Table(name, num_columns, key_index, self.bufferpool)
        self.tables.append(table)
        self._tables_by_name[name] = table
        return table

    """
    Deletes the specified table
    """
    def drop_table(self, name):
        # remove from list and dict; files are pruned by next close (not written)
        self.tables = [t for t in self.tables if t.name != name]
        if name in self._tables_by_name:
            del self._tables_by_name[name]
        return

    """
    Returns table with the passed name
    """
    def get_table(self, name):
        if name in self._tables_by_name:
            return self._tables_by_name[name]
        for table in self.tables:
            if table.name == name:
                self._tables_by_name[name] = table
                return table
        return None
