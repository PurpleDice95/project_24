package simpledb.common;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.storage.PageId;

public class LockManager {
    private final ConcurrentHashMap<PageId, Set<TransactionId>> sharedLocks;
    private final ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    ConcurrentHashMap<TransactionId, Set<TransactionId>> depGraph;
    private final Lock lock;

    public LockManager() {
        sharedLocks = new ConcurrentHashMap<>();
        exclusiveLocks = new ConcurrentHashMap<>();
        depGraph = new ConcurrentHashMap<TransactionId, Set<TransactionId>>();
        lock = new ReentrantLock();
    }

    // Method to acquire a lock
    public boolean acquireLock(PageId pid, TransactionId tid, Permissions perm) throws InterruptedException, TransactionAbortedException {
        lock.lock();
        try {
            if (perm == Permissions.READ_WRITE) {
                if (tid.equals(exclusiveLocks.get(pid))) return true;

                if (
                    exclusiveLocks.containsKey(pid) || 
                    (sharedLocks.containsKey(pid) && (sharedLocks.get(pid).size() > 1 || !sharedLocks.get(pid).contains(tid)))
                ) {
                    HashSet<TransactionId> holders = new HashSet<>();
                    if (exclusiveLocks.containsKey(pid)) {holders.add(exclusiveLocks.get(pid));}
                    if (sharedLocks.containsKey(pid)) {holders.addAll(sharedLocks.get(pid));}
                        

                    depGraph.put(tid, holders);
                    if (hasCycle(tid)) {
                        depGraph.remove(tid);
                        throw new TransactionAbortedException();
                    }
                }
                    

                exclusiveLocks.put(pid, tid);
                sharedLocks.clear();


                return true;

            } else if (perm == Permissions.READ_ONLY) {
                if (exclusiveLocks.containsKey(pid) && !exclusiveLocks.get(pid).equals(tid)) return false;
                // if (exclusiveLocks.containsKey(pid) ) {
                HashSet<TransactionId> holders = new HashSet<>();
                if (exclusiveLocks.containsKey(pid)) {holders.add(exclusiveLocks.get(pid));}
                if (sharedLocks.containsKey(pid)) {holders.addAll(sharedLocks.get(pid));}
                    
                // t1 -> {t2}, t2 -> {t1}
                depGraph.put(tid, holders);
                if (hasCycle(tid)) {
                    depGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
                // }

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
            if (depGraph.containsKey(tid)) depGraph.remove(tid);
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
    private synchronized boolean hasCycle(TransactionId tid) {
        Set<TransactionId> visited = new HashSet<TransactionId>();
        Queue<TransactionId> DfsQueue = new LinkedList<TransactionId>();
        visited.add(tid);

        DfsQueue.offer(tid);

        while (!DfsQueue.isEmpty()) {

            TransactionId Qhead = DfsQueue.poll();

            if (!depGraph.containsKey(Qhead)) {
                continue;
            }

            for (TransactionId node: depGraph.get(Qhead)) {
                // System.out.println(depGraph);
                // System.out.println(head);
                // System.out.println();
                if (node.equals(Qhead)) {
                    continue;
                }
                if (!visited.contains(node)) {
                    visited.add(node);
                    DfsQueue.offer(node);
                } else {
                    // Cycle deadlock
                    return true;
                }
            }
        }
        return false;
    }

}
