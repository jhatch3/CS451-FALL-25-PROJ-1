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
        self.key = key                 # primary key column index among user columns
        self.num_columns = num_columns #  user columns count. not metadata

        # RIDs
        self._next_base_rid = 1
        self._next_tail_rid = 1_000_000_000

        # in memory storage
        # rid -> row = [indirection, rid, timestamp, schema_mask, *user_values]
        self._rows = {}

        # base_rid -> newest tail rid (0 if none)
        self._head = {}

        # pk -> base rid
        self._pk = {}

        # base rid true if logically deleted
        self._deleted = {}

        self.allrecords = {} 

        # Optional Index (binary tree). This file does not depend on it.
        try:
            self.index = Index(self)
        except Exception:
            self.index = None

    # helpers
    def _now(self) -> int:
        return int(time())

    def _compose_row(self, indirection: int, rid: int, ts: int, schema: int, user_cols):
        return [indirection, rid, ts, schema] + user_cols

    def _latest_view(self, base_rid: int):
        """
        Return (latest_values_list, latest_schema_mask) for the given base rid.
        """
        base = self._rows[base_rid]
        values = base[META_COLS: META_COLS + self.num_columns][:]
        # Keep track of which columns have ever been updated
        schema_accum = base[SCHEMA_ENCODING_COLUMN]
        # Build lineage: newest -> older
        lineage = []
        cur = base[INDIRECTION_COLUMN]
        while cur:
            lineage.append(cur)
            cur = self._rows[cur][INDIRECTION_COLUMN]
        # Apply updates from oldest -> newest so the newest wins
        for tr in reversed(lineage):
            t = self._rows[tr]
            schema = t[SCHEMA_ENCODING_COLUMN]
            tvals = t[META_COLS: META_COLS + self.num_columns]
            for i in range(self.num_columns):
                if (schema >> i) & 1:
                    values[i] = tvals[i]
            schema_accum |= schema
        return values, schema_accum

    def _version_view(self, base_rid: int, relative_version: int):
        """
        Compute a historical snapshot:
          0   -> latest
         -1   -> base only
         -k   -> apply all tails except the newest (k-1) tails
        Returns (values, schema_mask_applied_up_to_that_point)
        """
        base = self._rows[base_rid]
        base_vals = base[META_COLS: META_COLS + self.num_columns][:]
        base_schema = base[SCHEMA_ENCODING_COLUMN]

        # Build lineage newest->older
        lineage = []
        cur = base[INDIRECTION_COLUMN]
        while cur:
            lineage.append(cur)
            cur = self._rows[cur][INDIRECTION_COLUMN]

        if relative_version == -1:
            return base_vals, base_schema
        if relative_version == 0:
            return self._latest_view(base_rid)

        if relative_version < 0:
            skip_newest_n = (-relative_version) - 1  #e.g. -2 => skip newest 1
            tails = lineage[::-1]  # oldest -> newest
            if skip_newest_n > 0:
                tails = tails[:-skip_newest_n]
            vals = base_vals[:]
            schema_accum = base_schema
            for tr in tails:
                t = self._rows[tr]
                schema = t[SCHEMA_ENCODING_COLUMN]
                tvals = t[META_COLS: META_COLS + self.num_columns]
                for i in range(self.num_columns):
                    if (schema >> i) & 1:
                        vals[i] = tvals[i]
                schema_accum |= schema
            return vals, schema_accum

        # Positive versions are not used in M1. treat as latest
        return self._latest_view(base_rid)

    # optional index hooks 
    def _index_add_pk(self, key_val: int, base_rid: int):
        if self.index is None:
            return
        try:
            # Dictionary style fallback
            if getattr(self.index, "indices", None) is not None:
                if self.index.indices[self.key] is None:
                    self.index.create_index(self.key)
                self.index.indices[self.key].setdefault(key_val, []).append(base_rid)
            else:
                # Plug in binary tree insert here later if needed
                pass
        except Exception:
            pass

    def _index_remove_pk(self, key_val: int, base_rid: int):
        if self.index is None:
            return
        try:
            if getattr(self.index, "indices", None) is not None:
                d = self.index.indices[self.key]
                if d is None:
                    return
                lst = d.get(key_val, [])
                if base_rid in lst:
                    lst.remove(base_rid)
                if not lst and key_val in d:
                    del d[key_val]
            else:
                # integrate with your binary tree delete API later
                pass
        except Exception:
            pass

    # M1 operations
    def insert(self, *columns) -> bool:
        """
        Insert a new base record. Return True on success; False on duplicate PK or wrong arity.
        """
        if len(columns) != self.num_columns:
            return False
        key_val = columns[self.key]
        if key_val in self._pk:
            return False  # reject duplicate primary keys

        rid = self._next_base_rid
        self._next_base_rid += 1

        row = self._compose_row(0, rid, self._now(), 0, list(columns))
        self._rows[rid] = row
        self._head[rid] = 0
        self._pk[key_val] = rid
        self._deleted[rid] = False
        self._index_add_pk(key_val, rid)
        return True

    def select(self, search_key: int, search_key_index: int, projected_columns):
        """
        Return [Record] for search_key == key on the PK column (M1 only supports PK lookups).
        projected_columns: list of 0/1 (length == num_columns)
        """
        if search_key_index != self.key:
            return []
        base_rid = self._pk.get(search_key)
        if not base_rid or self._deleted.get(base_rid, False):
            return []
        vals, schema_mask = self._latest_view(base_rid)
        projected = [v if sel else None for v, sel in zip(vals, projected_columns)]
        return [Record(base_rid, search_key, schema_mask, projected)]

    def select_version(self, search_key: int, search_key_index: int, projected_columns, relative_version: int):
        """
        Versioned select:
            0  -> latest
           -1  -> base
           -k  -> skip newest (k-1) tails
        """
        if search_key_index != self.key:
            return []
        base_rid = self._pk.get(search_key)
        if not base_rid or self._deleted.get(base_rid, False):
            return []
        vals, schema_mask = self._version_view(base_rid, relative_version)
        projected = [v if sel else None for v, sel in zip(vals, projected_columns)]
        return [Record(base_rid, search_key, schema_mask, projected)]

    def update(self, search_key: int, *columns) -> bool:
        """
        Update row by PK; pass None to skip a column.
        """
        if len(columns) != self.num_columns:
            return False
        base_rid = self._pk.get(search_key)
        if not base_rid or self._deleted.get(base_rid, False):
            return False

        current_vals, _ = self._latest_view(base_rid)
        new_vals = current_vals[:]
        schema = 0
        for i, v in enumerate(columns):
            if v is not None:
                new_vals[i] = v
                schema |= (1 << i)
        if schema == 0:
            return True  # nothing to change

        tail_rid = self._next_tail_rid
        self._next_tail_rid += 1

        prev_head = self._rows[base_rid][INDIRECTION_COLUMN]
        tail_row = self._compose_row(prev_head, tail_rid, self._now(), schema, new_vals)
        self._rows[tail_rid] = tail_row

        # patch base row
        base_row = self._rows[base_rid]
        base_row[INDIRECTION_COLUMN] = tail_rid
        base_row[SCHEMA_ENCODING_COLUMN] |= schema
        base_row[TIMESTAMP_COLUMN] = self._now()
        self._head[base_rid] = tail_rid

        return True

    def delete(self, search_key: int) -> bool:
        """
        Logical delete by PK (ignored by selects/sums).
        """
        base_rid = self._pk.get(search_key)
        if not base_rid or self._deleted.get(base_rid, False):
            return False
        self._deleted[base_rid] = True
        self._index_remove_pk(search_key, base_rid)
        return True

    def sum(self, start_key: int, end_key: int, column_index: int) -> int:
        """
        Sum the latest values of column_index for keys in [start_key, end_key].
        """
        if not (0 <= column_index < self.num_columns):
            return 0
        total = 0
        for k, rid in self._pk.items():
            if start_key <= k <= end_key and not self._deleted.get(rid, False):
                vals, _ = self._latest_view(rid)
                total += vals[column_index]
        return total

    def sum_version(self, start_key: int, end_key: int, column_index: int, relative_version: int) -> int:
        """
        Versioned sum computed via select_version snapshots.
        """
        if not (0 <= column_index < self.num_columns):
            return 0
        total = 0
        for k, rid in self._pk.items():
            if start_key <= k <= end_key and not self._deleted.get(rid, False):
                vals, _schema = self._version_view(rid, relative_version)
                total += vals[column_index]
        return total

    # placeholder for later 
    def __merge(self):
        # implement merge compaction later
        pass
