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
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        if None in columns:
            return False    #All columns should be passed a non-NULL value when inserting
        schema_encoding = '0' * self.table.num_columns
        # In the database, each record is assigned a unique identifier called a RID, which is often the physical
        # location of where the record is actually stored. In L-Store, this identifier will never change during
        # a record’s lifecycle

        #TODO Find out what to put for RID
        new_record = Record(rid=0, key=columns[0], columns=columns[1:], schema_encoding = schema_encoding)

        #TODO need to somehow incorporate pages
        self.table.allrecords[new_record.key] = new_record
        if self.table.allrecords[columns[0]] == new_record:
            return True
        else:
            return False 
 

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index): 
        pass

    
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
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):

        #TODO Probably change way of finding record using locate from index.py
        #     And do something with pages probably
        record = self.table.allrecords[primary_key]
        if record == None:
            return False
        new_encoding = ''
        for col_idx in range(self.table.num_columns-1):
            if record.columns[col_idx] != columns[col_idx]:
                new_encoding += '1'
            else:
                new_encoding += '0'
            
        record.schema_encoding = new_encoding       #Column updated so schema bit gets updated
        self.table.allrecords[primary_key] = record
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
