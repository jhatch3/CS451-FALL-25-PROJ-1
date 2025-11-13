"""
The Index class provides a data structure that allows fast processing of queries (e.g., select or
update) by indexing columns of tables over their values. Given a certain value for a column, the
index should efficiently locate all records having that value. The key column of all tables is
required to be indexed by default for performance reasons. However, supporting secondary indexes
is optional for this milestone. The API for this class exposes the two functions create_index and
drop_index (optional for this milestone).
"""

"""
A data strucutre holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # Store reference to table for num_columns and key
        self.table = table
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        # Always index the primary key column by default
        self.indices[table.key] = {}

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.indices[column] is None:
            return []
        else:
            return self.indices[column].get(value, [])

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            return []
        rids = []
        for val, rid_list in self.indices[column].items():
            if begin <= val <= end:
                rids.extend(rid_list)
        return rids

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = {}

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
