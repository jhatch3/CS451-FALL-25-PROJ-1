from lstore.table import Table, Record
from lstore.index import Index

"""
The Query class provides standard SQL operations such as insert, select, update, delete, and sum.
The select function returns all the records matching the search key (if any), and only the projected
columns of the matching records are returned. The insert function will insert a new record in the
table. All columns should be passed a non-NULL value when inserting. The update function
updates values for the specified set of columns. The delete function will delete the record with the
specified key from the table. The sum function will sum over the values of the selected column for
a range of records specified by their key values. We query tables by direct function calls rather
than parsing SQL queries.
"""

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        # Store a reference to the table we will run queries on

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key, txn_id = None):
        # Remove a record by its primary key
        return self.table.delete(primary_key, txn_id = txn_id)
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, txn_id = None):
        #Reject inserts with None values
        if None in columns:
            return False
        # Let the table handle duplicate keys and RID assignment
        return self.table.insert(*columns, txn_id=txn_id)

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index, txn_id = None):
        # Look up a record and only return requested columns
        return self.table.select(search_key, search_key_index, projected_columns_index, txn_id = txn_id)

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, txn_id = None):
        # Same as select, but lets you ask for an older version (e.g., -1 = base, 0 = latest)
        return self.table.select_version(search_key, search_key_index, projected_columns_index, relative_version)

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns, txn_id = None):
        # Update columns in a row by primary key. use none to leave a column unchanged
        return self.table.update(primary_key, *columns, txn_id = txn_id)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index, txn_id = None):
        #return the sum of one column for keys in a given rang. latest version
        return self.table.sum(start_range, end_range, aggregate_column_index)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, txn_id = None):
        # Same as sum, but computed over a specific version snapshot
        return self.table.sum_version(start_range, end_range, aggregate_column_index, relative_version)

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column, txn_id = None):
         #Increase the value of a single column by 1
        rows = self.select(key, self.table.key, [1] * self.table.num_columns)
        if not rows:
            return False
        current_val = rows[0].columns[column]
        updated_columns = [None] * self.table.num_columns
        updated_columns[column] = current_val + 1
        return self.update(key, *updated_columns)
