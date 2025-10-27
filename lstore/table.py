from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
META_COLS = 4



class Record:

    def __init__(self, rid, key, schema_encoding, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.schema_encoding = schema_encoding  #Each record has a schema encoding showing which ones updated


"""
The Table class provides the core of our relational storage functionality. All columns are 64-bit
integers in this implementation. Users mainly interact with tables through queries. Tables provide
a logical view of the actual physically stored data and mostly manage the storage and retrieval of
data. Each table is responsible for managing its pages and requires an internal page directory that,
given a RID, returns the actual physical location of the record. The table class should also manage
the periodical merge of its corresponding page ranges.
"""
class Table:
    """
    - single threaded, in memory L-store layout.
    - Base records hold full row. tail records hold snapshot + schema bitmask
    - Base.indirection -> newest tail RID (0 if none)
    - Tail.indirection -> previous tail RID (0 if none)
    - No physical page object usage for M1. We emulate via in-memory rows + a directory.
    - Binary tree Index hooks (optional)
    """
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.allrecords = {}        #TODO dont think this is correct
        pass

    def __merge(self):
        # TODO: implement merge compaction in later milestones.
        pass
 
