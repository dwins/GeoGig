package org.geogit.web.api.repo;

import java.io.IOException;
import java.io.Writer;

import org.geogit.api.GeoGIT;
import org.geogit.api.Ref;
import org.geogit.api.SymRef;
import org.geogit.api.plumbing.RefParse;
import org.geogit.api.porcelain.BranchListOp;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.representation.WriterRepresentation;
import org.restlet.resource.ServerResource;

import com.google.common.collect.ImmutableList;

public class ManifestResource extends ServerResource {
    {
        getVariants().add(new TextRepresentation());
    }
    
    private class TextRepresentation extends WriterRepresentation {
        public TextRepresentation() {
            super(MediaType.TEXT_PLAIN);
        }

        @Override
        public void write(Writer w) throws IOException {
            Form options = getRequest().getResourceRef().getQueryAsForm();
            
            boolean remotes = Boolean.valueOf(options.getFirstValue("remotes", "false"));

            GeoGIT ggit = (GeoGIT) getApplication().getContext().getAttributes().get("geogit");
            ImmutableList<Ref> refs = ggit.command(BranchListOp.class).setRemotes(remotes).call();

            // Print out HEAD first
            final Ref currentHead = ggit.command(RefParse.class).setName(Ref.HEAD).call().get();

            w.write(currentHead.getName() + " ");
            if (currentHead instanceof SymRef) {
                w.write(((SymRef) currentHead).getTarget());
            }
            w.write(" ");
            w.write(currentHead.getObjectId().toString());
            w.write("\n");

            // Print out the local branches
            for (Ref ref : refs) {
                w.write(ref.getName());
                w.write(" ");
                w.write(ref.getObjectId().toString());
                w.write("\n");
            }
            w.flush();
        }
    }
}
