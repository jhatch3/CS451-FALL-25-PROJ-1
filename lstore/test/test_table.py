# lstore/test/test_table.py
from lstore.db import Database
from lstore.query import Query

def run_tests():
    print("Creating DB/Table...")
    db = Database()
    t = db.create_table("Grades", 5, 0)   # 5 user columns, PK at column 0
    q = Query(t)

    print("Inserting...")
    assert q.insert(100, 1, 2, 3, 4)
    assert q.insert(101, 5, 6, 7, 8)
    assert not q.insert(100, 9, 9, 9, 9)   # duplicate PK

    print("Selecting...")
    r = q.select(100, t.key, [1,1,1,1,1])[0]
    assert r.columns == [100,1,2,3,4]

    print("Updating...")
    assert q.update(100, None, 10, None, 30, None)
    r2 = q.select(100, t.key, [1,1,1,1,1])[0]
    assert r2.columns == [100,10,2,30,4]

    print("Versioned select...")
    base = q.select_version(100, t.key, [1,1,1,1,1], -1)[0]
    latest = q.select_version(100, t.key, [1,1,1,1,1], 0)[0]
    assert base.columns == [100,1,2,3,4]
    assert latest.columns == [100,10,2,30,4]

    print("Summing...")
    assert q.sum(100, 101, 1) == 10 + 5

    print("Deleting...")
    assert q.delete(101)
    assert q.select(101, t.key, [1,1,1,1,1]) == []

    print("All basic tests passed!")

if __name__ == "__main__":
    run_tests()

