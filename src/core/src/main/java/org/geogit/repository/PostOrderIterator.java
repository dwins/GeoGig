package org.geogit.repository;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geogit.api.Bucket;
import org.geogit.api.Node;
import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.RevObject;
import org.geogit.api.RevTree;

import com.google.common.collect.AbstractIterator;

public class PostOrderIterator extends AbstractIterator<RevObject> {
    private final Repository repository;
    private List<List<ObjectId>> toVisit;
    private boolean down; // true when we're traversing backward through time, false on the return trip
    private final Successors successors;
    
    public static Iterator<RevObject> all(ObjectId top, Repository repository) {
        List<ObjectId> start = new ArrayList<ObjectId>();
        start.add(top);
        return new PostOrderIterator(start, repository, unique(ALL_SUCCESSORS));
    }
    
    public static Iterator<RevObject> range(
            List<ObjectId> start,
            List<ObjectId> base,
            Repository repository)
    {
        return new PostOrderIterator(
                new ArrayList<ObjectId>(start),
                repository,
                unique(blacklist(ALL_SUCCESSORS, base)));
    }

    private PostOrderIterator(List<ObjectId> start, Repository repository, Successors successors) {
        super();
        this.repository = repository;
        this.down = true;
        this.successors = successors;
        toVisit = new ArrayList<List<ObjectId>>();
        toVisit.add(new ArrayList<ObjectId>());
        toVisit.get(0).addAll(start);
    }

    @Override
    protected RevObject computeNext() {
        while (!toVisit.isEmpty()) {
            List<ObjectId> currentList = toVisit.get(0);
            if (currentList.isEmpty()) {
                down = false;
                toVisit.remove(0);
            } else {
                if (down) {
                    final ObjectId id = currentList.get(0);
                    final RevObject object = repository.getObjectDatabase().get(id);
                    final List<ObjectId> next = successors.findSuccessors(object);
                    toVisit.add(0, next);
                } else {
                    down = true;
                    final ObjectId id = currentList.remove(0);
                    if (successors.previsit(id)) {
                        return repository.getObjectDatabase().get(id);
                    }
                }
            }
        }
        return endOfData();
    }

    private static interface Successors {
        public List<ObjectId> findSuccessors(RevObject object);
        public boolean previsit(ObjectId id);
    }
    
    private final static Successors COMMIT_PARENTS = new Successors() {
        public List<ObjectId> findSuccessors(final RevObject object) {
            if (object instanceof RevCommit) {
                final RevCommit commit = (RevCommit) object;
                return new ArrayList<ObjectId>(commit.getParentIds());
            } else {
                return new ArrayList<ObjectId>();
            }
        }

        @Override
        public boolean previsit(ObjectId id) {
            return true;
        }
    };
    
    private final static Successors COMMIT_TREE = new Successors() {
        public List<ObjectId> findSuccessors(final RevObject object) {
            if (object instanceof RevCommit) {
                final RevCommit commit = (RevCommit) object;
                final ObjectId tree = commit.getTreeId();
                final List<ObjectId> results = new ArrayList<ObjectId>();
                results.add(tree);
                return results;
            } else {
                return new ArrayList<ObjectId>();
            }
        }

        @Override
        public boolean previsit(ObjectId id) {
            return true;
        }
    };

    private final static Successors TREE_FEATURES = new Successors() {
        public List<ObjectId> findSuccessors(final RevObject object) {
            if (object instanceof RevTree) {
                final RevTree tree = (RevTree) object;
                if (tree.features().isPresent()) {
                    final Set<ObjectId> seen = new HashSet<ObjectId>();
                    final List<ObjectId> results = new ArrayList<ObjectId>();
                    for (Node n : tree.features().get()) {
                        if (n.getMetadataId().isPresent()) {
                            if (seen.add(n.getMetadataId().get())) {
                                results.add(n.getMetadataId().get());
                            }
                        }
                        if (seen.add(n.getObjectId())) {
                            results.add(n.getObjectId());
                        }
                    }
                    return results;
                } else {
                    return new ArrayList<ObjectId>();
                }
            } else {
                return new ArrayList<ObjectId>();
            }
        }

        @Override
        public boolean previsit(ObjectId id) {
            return true;
        }
    };

    private final static Successors TREE_SUBTREES = new Successors() {
        public List<ObjectId> findSuccessors(final RevObject object) {
            if (object instanceof RevTree) {
                final RevTree tree = (RevTree) object;
                if (tree.trees().isPresent()) {
                    final Set<ObjectId> seen = new HashSet<ObjectId>();
                    final List<ObjectId> results = new ArrayList<ObjectId>();
                    for (Node n : tree.trees().get()) {
                        if (n.getMetadataId().isPresent()) {
                            if (seen.add(n.getMetadataId().get())) {
                                results.add(n.getMetadataId().get());
                            }
                        }
                        if (seen.add(n.getObjectId())) {
                            results.add(n.getObjectId());
                        }
                    }
                    return results;
                } else {
                    return new ArrayList<ObjectId>();
                }
            } else {
                return new ArrayList<ObjectId>();
            }
        }

        @Override
        public boolean previsit(ObjectId id) {
            return true;
        }
    };
    
    private final static Successors TREE_BUCKETS = new Successors() {
        public List<ObjectId> findSuccessors(final RevObject object) {
            if (object instanceof RevTree) {
                final RevTree tree = (RevTree) object;
                if (tree.buckets().isPresent()) {
                    final List<ObjectId> results = new ArrayList<ObjectId>();
                    for (Map.Entry<?, Bucket> entry : tree.buckets().get().entrySet()) {
                        final Bucket bucket = entry.getValue();
                        results.add(bucket.id());
                    }
                    return results;
                } else {
                    return new ArrayList<ObjectId>();
                }
            } else {
                return new ArrayList<ObjectId>();
            }
        }

        @Override
        public boolean previsit(ObjectId id) {
            return true;
        }
    };
    
    private final static Successors combine(final Successors... chained) {
        return new Successors() {
            public List<ObjectId> findSuccessors(final RevObject object) {
                final List<ObjectId> results = new ArrayList<ObjectId>();
                for (Successors s : chained) {
                    results.addAll(s.findSuccessors(object));
                }
                return results;
            }
            
            public boolean previsit(ObjectId id) {
                for (Successors s : chained) {
                    if (!s.previsit(id)) return false;
                }
                return true;
            }
        };
    }
    
    private final static Successors unique(final Successors delegate) {
        final Set<ObjectId> seen = new HashSet<ObjectId>();
        return new Successors() {
            public List<ObjectId> findSuccessors(final RevObject object) {
                if (seen.contains(object.getId())) {
                    return new ArrayList<ObjectId>();
                } else {
                    final List<ObjectId> results = delegate.findSuccessors(object);
                    results.removeAll(seen);
                    return results;
                }
            }
            
            public boolean previsit(ObjectId id) {
                return seen.add(id) && delegate.previsit(id);
            }
        };
    }

    private final static Successors blacklist(
            final Successors delegate, final List<ObjectId> base)
    {
        final Set<ObjectId> baseSet = new HashSet<ObjectId>(base);
        return new Successors() {
            public List<ObjectId> findSuccessors(final RevObject object) {
                if (baseSet.contains(object.getId())) {
                    return new ArrayList<ObjectId>();
                } else {
                    List<ObjectId> results = delegate.findSuccessors(object);
                    Set<ObjectId> removed = new HashSet<ObjectId>(baseSet);
                    removed.retainAll(results);
                    results.removeAll(baseSet);
                    return results;
                }
            }
            
            public boolean previsit(ObjectId id) {
                boolean dprevisit = delegate.previsit(id);
                return dprevisit && !baseSet.contains(id);
            }
        };
    }
    
    private final static Successors ALL_SUCCESSORS =
            combine( //
                COMMIT_PARENTS, //
                COMMIT_TREE, //
                TREE_BUCKETS, //
                TREE_SUBTREES, // 
                TREE_FEATURES);
}
