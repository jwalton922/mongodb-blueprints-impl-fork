package be.datablend.blueprints.impls.mongodb;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import java.util.Iterator;
import java.util.List;
import static be.datablend.blueprints.impls.mongodb.util.MongoDBUtil.MONGODB_ID;
import com.tinkerpop.blueprints.Predicate;
import java.util.NoSuchElementException;
import java.util.Vector;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class MongoDBIterable<T extends Element> implements CloseableIterable<T> {

    private DBCursor cursorIds;
    private List<Object> ids;
    private final MongoDBGraph graph;
    private Class<T> clazz;
    private List<PredicateContainer> predicates;
    private long limit = 0;

    public MongoDBIterable(final DBCursor cursorIds, final MongoDBGraph graph, final Class<T> clazz, List<PredicateContainer> predicates, int limit) {
        this.cursorIds = cursorIds;
        this.graph = graph;
        this.clazz = clazz;
        this.predicates = predicates;
        this.limit = limit;
    }

    public MongoDBIterable(final List<Object> ids, final MongoDBGraph graph, final Class<T> clazz, List<PredicateContainer> predicates, int limit) {
        this.graph = graph;
        this.ids = ids;
        this.clazz = clazz;
        this.predicates = predicates;
        this.limit = limit;
    }

    @Override
    public Iterator<T> iterator() {
        if (cursorIds != null) {
            return new MongoDBCursorIterator();
        } else {
            return new MongoDBIdIterator();
        }
    }

    @Override
    public void close() {
    }

    private class MongoDBCursorIterator implements Iterator<T> {

        private Iterator<DBObject> iterator = cursorIds.iterator();
        private DBObject nextRetValue = null;
        private long count = 0;

        @Override
        public boolean hasNext() {
            if (nextRetValue != null) {
                return true;
            }
            if (null != this.nextRetValue) {
                return true;
            } else {
                return this.loadNext();
            }
        }

        private boolean loadNext() {
            this.nextRetValue = null;
            if (this.count > limit) {
                return false;
            }
            while (this.iterator.hasNext()) {
                DBObject nextElem = this.iterator.next();
                boolean keep = passesPredicate(nextElem);
                if (keep) {
                    if (++this.count <= limit) {
                        this.nextRetValue = nextElem;
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean passesPredicate(DBObject retObject) {
            boolean passesPredicates = true;
            for (int i = 0; i < predicates.size(); i++) {
                System.out.println("Evaluating predicate: " + i + " obj: " + retObject.toString() + " value: " + predicates.get(i).getObject());
                String key = predicates.get(i).getKey();
                Predicate predicate = predicates.get(i).getPredicate();
                Object value = predicates.get(i).getObject();
                Object retObjValue = retObject.get(key);

                if (!predicate.evaluate(retObjValue, value)) {
                    passesPredicates = false;
                    System.out.println("Failed predicate!");
                    break;
                }
                System.out.println("passed predicate");
            }

            return passesPredicates;
        }

        @Override
        public T next() {
            while (true) {
                if (this.nextRetValue != null) {
                    DBObject temp = this.nextRetValue;
                    this.nextRetValue = null;
                    Object object = temp.get(MONGODB_ID);
                    T ret = null;
                    if (clazz == Vertex.class) {
                        ret = (T) new MongoDBVertex(graph, object);
                    } else if (clazz == Edge.class) {
                        ret = (T) new MongoDBEdge(graph, object);
                    } else {
                        throw new IllegalStateException();
                    }
                    return ret;
                }
                if(!this.loadNext()){
                    throw new NoSuchElementException();
                }
            }         
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class MongoDBIdIterator implements Iterator<T> {

        private Iterator<Object> iterator = ids.iterator();

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            T ret = null;
            if (clazz == Vertex.class) {
                ret = (T) new MongoDBVertex(graph, iterator.next());
            } else if (clazz == Edge.class) {
                ret = (T) new MongoDBEdge(graph, iterator.next());
            } else {
                throw new IllegalStateException();
            }
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
