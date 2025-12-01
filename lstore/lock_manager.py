"""
Minimal lock manager used by Database/Transactions.
Current implementation is a no-op placeholder that tracks locks per transaction
so higher layers can call acquire/release/release_all without import errors.
"""

from collections import defaultdict
from threading import Lock


class LockManager:
    def __init__(self):
        self._locks = defaultdict(set)  # txn_id -> set of rids locked
        self._mtx = Lock()

    def acquire(self, txn_id, rid, mode="S"):
        """
        Acquire a lock on rid for txn_id. This simplified version always
        succeeds and just records the ownership.
        """
        if self.already_locked(txn_id, rid) == True:
            #print("Already locked")
            return False
        
        with self._mtx:
            self._locks[txn_id].add(rid)
        return True

    def release(self, txn_id, rid):
        """Release a single lock."""
        with self._mtx:
            if rid in self._locks.get(txn_id, ()):
                self._locks[txn_id].discard(rid)
                if not self._locks[txn_id]:
                    del self._locks[txn_id]
        

    def release_all(self, txn_id):
        """Release all locks held by txn_id."""
        with self._mtx:
            self._locks.pop(txn_id, None)
    
    def already_locked(self, txn_id, rid):
        """Check if theres already a lock for the rid"""
        with self._mtx:
            if rid in self._locks.get(txn_id, ()):
                return True
            else: 
                return False
