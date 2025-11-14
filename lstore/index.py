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

from lstore.BinaryTree import Tree

class Index:

    def __init__(self, table):
        # Store reference to table for num_columns and key
        self.table = table
        # One tree per column: Tree() for BST with multi-keys (RIDs) per value
        self.indices = []
        for _ in range(table.num_columns):
            self.indices.append(Tree())
        # Primary key is already indexed (empty tree)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if 0 <= column < len(self.indices):
            node = self.indices[column].find_node(value)
            return node.keys if node else []
        return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if 0 <= column < len(self.indices):
            nodes = self.indices[column].find_node_range(begin, end)
            rids = []
            for node in nodes:
                if node:
                    rids.extend(node.keys)
            return rids
        return []

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
        if 0 <= column_number < len(self.indices):
            self.indices[column_number] = Tree()  # Reset to new empty tree

    # Add a base RID for a value in a column
    def add(self, column, value, base_rid):
        if value is None:  # Skip NULLs/sentinels
            return
        if 0 <= column < len(self.indices):
            self.indices[column].insert(value, base_rid)

    # Remove a base RID for a value in a column
    def remove(self, column, value, base_rid):
        if value is None or not (0 <= column < len(self.indices)):
            return
        try:
            self.indices[column].delete(value, base_rid)
        except (ValueError, KeyError):
            pass  # Ignore if not found
