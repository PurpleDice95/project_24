package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;


import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> aggregateMap;
    private int totalCount;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.totalCount = 0;
        this.aggregateMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            totalCount++;
        } else {
            if (aggregateMap.get(tup.getField(gbfield)) == null) {
                aggregateMap.put(tup.getField(gbfield), 1);
            } else {
                aggregateMap.put(tup.getField(gbfield), aggregateMap.get(tup.getField(gbfield)) + 1 );
            }
            
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
                iter = aggregateMap.entrySet().iterator();
                flag = false;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext() || (gbfield == NO_GROUPING && !flag);
            }

            @Override
            public Tuple next() {
                Tuple tuple = new Tuple(td);
                if (gbfield == NO_GROUPING) {
                    tuple.setField(0, new IntField(totalCount));
                    flag = true;
                } else {
                    Map.Entry<Field, Integer> entry = iter.next();
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(entry.getValue()));
                }
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

    // Helper Functions below
    private int getUpdatedCount(Tuple tup) {
        Field groupByField = getGroupByField(tup);
        int currentCount = this.aggregateMap.getOrDefault(groupByField, 0);
        return ++currentCount;
    }
    private Field getGroupByField(Tuple tup) {
        Field groupByField;
        if (this.gbfield == Aggregator.NO_GROUPING) {
            groupByField = null;
        } else {
            groupByField = tup.getField(this.gbfield);
        }
        return groupByField;
    }

}
