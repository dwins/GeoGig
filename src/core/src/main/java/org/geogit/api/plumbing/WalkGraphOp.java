package org.geogit.api.plumbing;

import java.util.Iterator;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.GeoGIT;
import org.geogit.api.ObjectId;
import org.geogit.api.RevObject;
import org.geogit.repository.PostOrderIterator;

import com.google.common.base.Optional;
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
        Optional<ObjectId> ref = command(RevParse.class).setRefSpec(reference).call();
        if (!ref.isPresent()) return Iterators.emptyIterator();
        return PostOrderIterator.all(ref.get(), ggit.getRepository().getObjectDatabase());
    }
}
