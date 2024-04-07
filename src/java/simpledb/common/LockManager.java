package simpledb.common;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.transaction.TransactionId;
import simpledb.storage.PageId;

public class LockManager {
    private final ConcurrentHashMap<PageId, Set<TransactionId>> sharedLocks;
    private final ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    private final Lock lock;

    public LockManager() {
        sharedLocks = new ConcurrentHashMap<>();
        exclusiveLocks = new ConcurrentHashMap<>();
        lock = new ReentrantLock();
    }

    // Method to acquire a lock
    public boolean acquireLock(PageId pid, TransactionId tid, Permissions perm) throws InterruptedException {
        lock.lock();
        try {
            if (perm == Permissions.READ_WRITE) {
                if (tid.equals(exclusiveLocks.get(pid))) return true;

                if (
                    exclusiveLocks.containsKey(pid) || 
                    (sharedLocks.containsKey(pid) && (sharedLocks.get(pid).size() > 1 || !sharedLocks.get(pid).contains(tid)))
                ) return false;

                exclusiveLocks.put(pid, tid);
                sharedLocks.clear();

                return true;

            } else if (perm == Permissions.READ_ONLY) {
                if (exclusiveLocks.containsKey(pid) && !exclusiveLocks.get(pid).equals(tid)) return false;

                sharedLocks.computeIfAbsent(pid, k -> new HashSet<>()).add(tid);
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    // Method to release a lock
    public void releaseLock(PageId pid, TransactionId tid) {
        lock.lock();
        try {
            // Remove from shared locks
            if (sharedLocks.containsKey(pid)) {
                Set<TransactionId> tids = sharedLocks.get(pid);
                tids.remove(tid);
                if (tids.isEmpty()) {
                    sharedLocks.remove(pid);
                }
            }
            // Remove from exclusive locks
            if (tid.equals(exclusiveLocks.get(pid))) {
                exclusiveLocks.remove(pid);
            }
        } finally {
            lock.unlock();
        }
    }

    // Method to check if a transaction holds a lock
    public boolean holdsLock(PageId pid, TransactionId tid) {
        lock.lock();
        try {
            return sharedLocks.getOrDefault(pid, Collections.emptySet()).contains(tid) ||
                   tid.equals(exclusiveLocks.get(pid));
        } finally {
            lock.unlock();
        }
    }

    public boolean pageHasLock(PageId pid) {
        lock.lock();
        try {
            return (exclusiveLocks.containsKey(pid) || sharedLocks.containsKey(pid));
        } finally {
            lock.unlock();
        }
    }

    public void releaseTransactionsLocks(TransactionId tid) {
        lock.lock();
        try {
            for (PageId pid : sharedLocks.keySet()) {
                sharedLocks.get(pid).remove(tid);
            }

            exclusiveLocks.entrySet().removeIf(entry -> entry.getValue().equals(tid));
        } finally {
            lock.unlock();
        }
    }

}
