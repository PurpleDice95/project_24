package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.IntField;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.io.IOException;


/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId transactionId;
    private OpIterator child;
    private int tableId;
    private boolean fetched;
    private TupleDesc td;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        fetched = false;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});

        // Check if the TupleDesc of the child differs from the table into which we are to insert
        TupleDesc childTupleDesc = child.getTupleDesc();
        TupleDesc tableTupleDesc = Database.getCatalog().getTupleDesc(tableId);
        if (!childTupleDesc.equals(tableTupleDesc)) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert");
        }
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException{
        if (fetched) {
            return null;
        }

        int insertedRecords = 0;
        try {
            while (child.hasNext()) {
                Tuple tuple = child.next();
                Database.getBufferPool().insertTuple(transactionId, tableId, tuple);
                insertedRecords++;
            }
        } catch (IOException e) {
            e.printStackTrace(); 
        }

        // Create a tuple containing the number of inserted records
        Tuple resultTuple = new Tuple(getTupleDesc());
        resultTuple.setField(0, new IntField(insertedRecords));
        fetched = true;
        return resultTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Expected exactly one child");
        }
        child = children[0];
    }
}
