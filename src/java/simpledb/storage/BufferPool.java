package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    
    private int numPages;
    private ConcurrentHashMap<PageId, Page> bufferPool;
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.bufferPool = new ConcurrentHashMap<>(numPages);
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        try {
            // Block and acquire the lock before fetching the page
            boolean lockAcquired = lockManager.acquireLock(pid, tid, perm);
            if (lockAcquired) {
                if (!bufferPool.containsKey(pid)) {
                    // Page not in buffer pool, fetch from disk
                    DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    Page page = dbFile.readPage(pid);
                    if (bufferPool.size() >= numPages) {
                        evictPage(); // Evict a page if buffer pool is full
                        // throw new DbException("Buffer pool is full");
                    }
                    bufferPool.put(pid, page);
                }
                return bufferPool.get(pid);
            } else {
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransactionAbortedException();
        }
        
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            // Commit: Flush dirty pages related to this transaction to disk
            try {
                flushPages(tid);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } else {
            // Abort: Revert changes made by this transaction
            for (PageId pid : bufferPool.keySet()) {
                Page page = bufferPool.get(pid);
                if (page.isDirty() != null && page.isDirty().equals(tid)) {
                    // Discard the dirty page by reading a fresh copy from the disk
                    Page freshPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    // Replace the current page in the buffer pool with the fresh copy
                    bufferPool.put(pid, freshPage);
                }
            }
        }
        // Release locks held by this transaction
        lockManager.releaseTransactionsLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        if (t == null) {
            throw new DbException("Tuple is null");
        }
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        if (heapFile == null) {
            throw new DbException("HeapFile is null");
        }
        List<Page> affectedPages = heapFile.insertTuple(tid, t);

        for (Page pg : affectedPages) {
            pg.markDirty(true, tid);
            if (!bufferPool.containsKey(pg.getId()) && bufferPool.size() >= numPages) {
                evictPage();
            }
            bufferPool.remove(pg.getId());
            bufferPool.put(pg.getId(), pg);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        if (t == null) {
            throw new DbException("Tuple is null");
        }
        PageId pid = t.getRecordId().getPageId();
        if (pid == null) {
            throw new DbException("PageId is null");
        }
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        if (heapFile == null) {
            throw new DbException("HeapFile is null");
        }
        List<Page> affectedPages = heapFile.deleteTuple(tid, t);
        
        for (Page pg : affectedPages) {
            pg.markDirty(true, tid);
            if (!bufferPool.containsKey(pg.getId()) && bufferPool.size() >= numPages) {
                evictPage();
            }
            bufferPool.remove(pg.getId());
            bufferPool.put(pg.getId(), pg);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : bufferPool.keySet()) {
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = bufferPool.get(pid);
        if (page == null) {
            throw new IOException("Page not found in buffer pool");
        }

        if (page.isDirty() != null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            dbFile.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        System.out.println(bufferPool.toString());
        for (PageId pid : bufferPool.keySet()) {
            Page page = bufferPool.get(pid);
            // Check if the page is dirty and if the transaction is the one that dirtied it
            System.out.println(page.isDirty());
            if (page.isDirty() != null && page.isDirty().equals(tid)) {
                System.out.println(pid);
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        PageId victimPid = null;
        // LRU, hashmap order
        for (PageId pid : bufferPool.keySet()) {
            Page page = bufferPool.get(pid);
            if (page.isDirty() == null) {
                victimPid = pid;
                break;
            }
        }

        // If all pages are dirty, throw an exception
        if (victimPid == null) {
            throw new DbException("All pages in buffer pool are dirty");
        }

        // Flush the victim page to disk before eviction
        try {
            flushPage(victimPid);
        } catch (IOException e) {
            throw new DbException("Error flushing page during eviction: " + e.getMessage());
        }

        // Remove the victim page from the buffer pool
        bufferPool.remove(victimPid);
    
    }

}
