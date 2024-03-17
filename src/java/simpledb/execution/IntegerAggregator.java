package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;

import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.TupleDesc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> aggregateMap;
    private Map<Field, Integer> avgCounts;
    private int totalCount;
    private int totalValue;
    private boolean startedFlag;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregateMap = new HashMap<>();
        this.avgCounts = new HashMap<>();
        this.startedFlag=false;
        this.totalCount = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int aggregateValue = ((IntField) tup.getField(afield)).getValue();

        if (gbfield == NO_GROUPING) {
            if (!startedFlag) {
                startedFlag = true;
                totalValue = aggregateValue;
            } else {
                if (what == Op.AVG) {
                    totalValue = applyAvg(totalValue, aggregateValue, totalCount + 1);
                } else {
                    totalValue = applyOp(totalValue, aggregateValue);
                }
            }
            totalCount++;
        } else {
            if (aggregateMap.get(tup.getField(gbfield)) == null) {
                if (what == Op.AVG) {
                    avgCounts.put(tup.getField(gbfield), 1);
                }
                aggregateMap.put(tup.getField(gbfield), aggregateValue);
                
            } else {
                if (what == Op.AVG) {
                    avgCounts.put(tup.getField(gbfield), avgCounts.get(tup.getField(gbfield)) + 1);
                    aggregateMap.put(
                        tup.getField(gbfield), 
                        applyAvg(aggregateMap.get(tup.getField(gbfield)), aggregateValue, avgCounts.get(tup.getField(gbfield)))
                    );
                } else {
                    aggregateMap.put(tup.getField(gbfield), applyOp(aggregateMap.get(tup.getField(gbfield)), aggregateValue));
                }
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        return new OpIterator() {
            private Iterator<Map.Entry<Field, Integer>> iter = aggregateMap.entrySet().iterator();
            private boolean flag;

            @Override
            public void open() {
                flag = false;
                iter = aggregateMap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext() || (gbfield == NO_GROUPING && !flag);
            }

            @Override
            public Tuple next() {
                Tuple tuple = new Tuple(td);
                if (gbfield == NO_GROUPING) {
                    tuple.setField(0, new IntField(totalValue));
                    flag = true;
                } else {
                    Map.Entry<Field, Integer> entry = iter.next();
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(entry.getValue()));
                }
                // System.out.println(tuple);
                return tuple;
            }

            @Override
            public void rewind() {
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                iter = null;
                flag = false;
            }
        };
    }

     private int applyOp(int value1, int value2) {
        switch (what) {
            case MIN:
                return Math.min(value1, value2);
            case MAX:
                return Math.max(value1, value2);
            case SUM:
                return value1 + value2;
            case COUNT:
                return value1 + 1;
            default:
                throw new UnsupportedOperationException("Unknown operation: " + what);
        }
    }

    private int applyAvg(int value1, int value2, int currCount) {
        return ((value1 * (currCount-1)) + value2)/currCount;
    }
}
