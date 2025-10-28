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


def test_edges():
    print("Running edge tests...")
    db = Database()
    t = db.create_table("Edge", 5, 0)
    q = Query(t)

    # wrong arity / None not allowed
    assert q.insert(1,2,3,4) is False
    assert q.insert(1,2,3,4,None) is False

    # valid insert
    assert q.insert(10,100,200,300,400) is True

    # PK-only lookup works
    assert q.select(10, 1, [1,1,1,1,1]) == []  # searching on non-PK col returns []

    # projected columns respected
    proj = q.select(10, t.key, [1,0,1,0,1])[0].columns
    assert proj == [10, None, 200, None, 400]   # keep placeholders for unprojected cols


    # update no-op (all None) leaves unchanged
    before = q.select(10, t.key, [1,1,1,1,1])[0].columns
    assert q.update(10, None, None, None, None, None) is True
    after = q.select(10, t.key, [1,1,1,1,1])[0].columns
    assert before == after

    # multiple updates, versioning
    assert q.update(10, None, 111, None, None, None) is True
    assert q.update(10, None, None, 222, None, None) is True
    latest = q.select(10, t.key, [1,1,1,1,1])[0].columns
    assert latest == [10,111,222,300,400]
    minus2 = q.select_version(10, t.key, [1,1,1,1,1], -2)[0].columns
    assert minus2 == [10,111,200,300,400]

    # delete excludes from sum
    assert q.insert(20,1,2,3,4) is True
    assert q.delete(20) is True
    assert q.sum(10,20,1) == 111

    print("All edge tests passed!")


if __name__ == "__main__":
    run_tests()
    test_edges()
    print("All tests passed")



