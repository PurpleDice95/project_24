package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private OpIterator child;
    private int afield;
    private Aggregator.Op aop;
    private Aggregator agg;
    private OpIterator aggIter;
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.gbfield = gfield;
        this.afield = afield;
        this.child = child;
        this.aop = aop;
        if (child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE) {
            this.agg = new StringAggregator(groupField(), (gfield == -1)? null: child.getTupleDesc().getFieldType(gbfield), afield, aop);
        } else {
            this.agg = new IntegerAggregator(groupField(), (gfield == -1)? null: child.getTupleDesc().getFieldType(gbfield), afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return (gbfield == -1)? Aggregator.NO_GROUPING: gbfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return (this.groupField() == Aggregator.NO_GROUPING)? null: child.getTupleDesc().getFieldName(gbfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        while (child.hasNext()) {
            agg.mergeTupleIntoGroup(child.next());
        }
        aggIter = agg.iterator();
        aggIter.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        return (aggIter.hasNext())? aggIter.next(): null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.aggIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return new TupleDesc(
            (this.groupField() == Aggregator.NO_GROUPING)?    
                new Type[] {child.getTupleDesc().getFieldType(afield)} :
                new Type[] {child.getTupleDesc().getFieldType(gbfield), child.getTupleDesc().getFieldType(afield)}
            ,
            (this.groupField() == Aggregator.NO_GROUPING)? 
                new String[] {aop.toString() + " (" + this.aggregateFieldName() + ")"}:
                new String[] {this.groupFieldName(), aop.toString() + " (" + this.aggregateFieldName() + ")"}
        );
    }

    public void close() {
        // some code goes here
        super.close();
        this.aggIter.close();
        this.child.close();
        this.aggIter = null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1)
            throw new IllegalArgumentException("Expected only one child operator");
        
        this.child = children[0];
    }

}
