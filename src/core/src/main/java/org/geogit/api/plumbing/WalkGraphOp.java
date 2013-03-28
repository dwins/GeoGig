package org.geogit.api.plumbing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.Bucket;
import org.geogit.api.GeoGIT;
import org.geogit.api.Node;
import org.geogit.api.ObjectId;
import org.geogit.api.Ref;
import org.geogit.api.RevCommit;
import org.geogit.api.RevObject;
import org.geogit.api.RevTree;
import org.geogit.repository.Repository;

import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;

public final class WalkGraphOp extends AbstractGeoGitOp<Iterator<RevObject>> {
    private final GeoGIT ggit;
    
    private String reference;
    
    public WalkGraphOp setReference(final String reference) {
        this.reference = reference;
        return this;
    }
    
    @Inject
    public WalkGraphOp(GeoGIT ggit) {
        this.ggit = ggit;
    }

    @Override
    public Iterator<RevObject> call() {
        Optional<Ref> ref = command(RefParse.class).setName(reference).call();
        if (!ref.isPresent()) return Iterators.emptyIterator();
        return new PostOrderIterator(ref.get().getObjectId(), ggit.getRepository());
    }
    
    private static class PostOrderIterator extends AbstractIterator<RevObject> {
        private final Repository repository;
        private List<List<ObjectId>> toVisit;
        private boolean down; // true when we're traversing backward through time, false on the return trip

        public PostOrderIterator(ObjectId top, Repository repository) {
            super();
            this.repository = repository;
            this.down = true;
            toVisit = new ArrayList<List<ObjectId>>();
            toVisit.add(new ArrayList<ObjectId>());
            toVisit.get(0).add(top);
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
                        toVisit.add(0, computeSuccessorIds(currentList.get(0)));
                    } else {
                        down = true;
                        final ObjectId id = currentList.remove(0);
                        return repository.getObjectDatabase().get(id);
                    }
                }
            }
            return endOfData();
        }
        
        private List<ObjectId> computeSuccessorIds(ObjectId id) {
            final RevObject object = repository.getObjectDatabase().get(id);
            if (object instanceof RevCommit) {
                RevCommit commit = (RevCommit) object;
                List<ObjectId> results = new ArrayList<ObjectId>(commit.getParentIds());
                results.add(commit.getTreeId());
                return results;
            } else if (object instanceof RevTree) {
                RevTree tree = (RevTree) object;
                Set<ObjectId> results = new HashSet<ObjectId>();
                if (tree.features().isPresent()) {
                    for (Node n : tree.features().get()) {
                        results.add(n.getObjectId());
                        if (n.getMetadataId().isPresent()) {
                            results.add(n.getMetadataId().get());
                        }
                    }
                }
                if (tree.trees().isPresent()) {
                    for (Node n : tree.trees().get()) {
                        results.add(n.getObjectId());
                        if (n.getMetadataId().isPresent()) {
                            results.add(n.getMetadataId().get());
                        }
                    }
                }
                if (tree.buckets().isPresent()) {
                    for (Bucket b : tree.buckets().get().values()) {
                        results.add(b.id());
                    }
                }
                return new ArrayList<ObjectId>(results);
            } else {
                return new ArrayList<ObjectId>();
            }
        }
    }
}
