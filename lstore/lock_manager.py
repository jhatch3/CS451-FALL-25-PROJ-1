from __future__ import annotations

import threading
from typing import Dict, Hashable, Optional, Set

# transaction identifier (int)
TxnId = int

# Resource identifier can be anything hashable
# Usually something like the table name plus the record id
ResourceId = Hashable


class LockManager:
    """
    lock manager

    Quick overview:
    - Shared (S) locks. multiple txns can read the same record.
    - Exclusive (X) locks.only one txn can write to it.
    - No-wait. if the lock isn’t available right away, we just return False.
    - Strict 2PL. we hold all locks until commit/abort, then release everything.

     thread safe because uses mutex.

    Main functions that get called
    - acquire_s(txn, res): get a shared/read lock
    - acquire_x(txn, res): get an exclusive/write lock
    - release_all(txn): drop all locks for that transaction
    """

    def __init__(self):
        self._mu = threading.Lock()  # protects all the dicts below
        # resource -> set of S holders, X holder
        self._locks: Dict[ResourceId, tuple[Set[TxnId], Optional[TxnId]]] = {}
        # txn -> set of resources it locked for fast cleanup
        self._txn_to_resources: Dict[TxnId, Set[ResourceId]] = {}

    # Public functions
    #-----------------
    def acquire_s(self, txn: TxnId, res: ResourceId) -> bool:
        """
        Grab a shared (read) lock on a resource.
        Returns True if we got it, False if someone else has an exclusive lock.
        """
        with self._mu:
            s_holders, x_holder = self._locks.setdefault(res, (set(), None))
            
            # can't get S lock if someone else has X lock
            if x_holder is not None and x_holder != txn:
                return False
            
            # add ourselves to the shared holders
            s_holders.add(txn)
            self._txn_to_resources.setdefault(txn, set()).add(res)
            return True

    def acquire_x(self, txn: TxnId, res: ResourceId) -> bool:
        """
        Grab an exclusive write lock on a resource.
        returns true if we got it, false if theres a conflict.

        If we already have the X lock, just return true
        If someone else has X, we can't get it.
        If we have S and want to upgrade to X, we can only do that if we're the only S holder
        """
        with self._mu:
            s_holders, x_holder = self._locks.setdefault(res, (set(), None))
            
            # if already have X lock we're fine
            if x_holder == txn:
                self._txn_to_resources.setdefault(txn, set()).add(res)
                return True
            
            # if someone else has X lock we cant get it
            if x_holder is not None and x_holder != txn:
                return False
            
            # trying to upgrade from S to X. only works if we're the only S holder
            if s_holders and s_holders != {txn}:
                return False
            
            # upgrade: remove ourselves from S holders, give ourselves X
            s_holders.discard(txn)
            self._locks[res] = (s_holders, txn)
            self._txn_to_resources.setdefault(txn, set()).add(res)
            return True

    def release_all(self, txn: TxnId) -> None:
        """
        Release all locks held by this transaction.
        I call this when a transaction commits or aborts.
        Also cleans up empty lock states to save memory.
        """
        with self._mu:
            resources = self._txn_to_resources.pop(txn, None)
            if not resources:
                return
            
            # go through each resource this txn locked and release it
            for res in resources:
                if res not in self._locks:
                    continue
                
                s_holders, x_holder = self._locks[res]
                
                # remove from shared holders
                s_holders.discard(txn)
                
                # clear exclusive lock if we had it
                if x_holder == txn:
                    x_holder = None
                
                # if nobody has any locks on this resource, delete the entry
                if not s_holders and x_holder is None:
                    del self._locks[res]
                else:
                    # update the lock state
                    self._locks[res] = (s_holders, x_holder)

    # ---------------------------------------------------------------
    # Convenience methods mostly ofr backward compatibility
    # ---------------------------------------------------------------

    def acquire(self, txn_id: TxnId, rid: ResourceId, mode: str = "S") -> bool:
        """
        Convenience method that calls acquire_s or acquire_x based on mode.
        I kept this so old code that uses acquire with mode S or X still works
        Accepts both uppercase and lowercase.
        """
        mode_upper = mode.upper()
        if mode_upper == "S":
            return self.acquire_s(txn_id, rid)
        elif mode_upper == "X":
            return self.acquire_x(txn_id, rid)
        else:
            return False

    def release(self, txn_id: TxnId, rid: ResourceId) -> None:
        """
        release one lock on a single resource.
        with strict 2PL we can release all locks at once using release_all,
        but I left this in case we want to drop just one lock.
        """
        with self._mu:
            # rdrop the resource from the list for this txn
            if txn_id in self._txn_to_resources:
                self._txn_to_resources[txn_id].discard(rid)
                if not self._txn_to_resources[txn_id]:
                    del self._txn_to_resources[txn_id]
            
            # remove from lock state
            if rid not in self._locks:
                return
            
            s_holders, x_holder = self._locks[rid]
            s_holders.discard(txn_id)
            if x_holder == txn_id:
                x_holder = None
            
            # clean up if empty
            if not s_holders and x_holder is None:
                del self._locks[rid]
            else:
                self._locks[rid] = (s_holders, x_holder)
