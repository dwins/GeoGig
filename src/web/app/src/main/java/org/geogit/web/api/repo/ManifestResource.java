package org.geogit.web.api.repo;

import java.io.IOException;
import java.io.Writer;

import org.geogit.api.GeoGIT;
import org.geogit.api.ObjectId;
import org.geogit.api.Ref;
import org.geogit.api.SymRef;
import org.geogit.api.plumbing.RefParse;
import org.geogit.api.porcelain.BranchListOp;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.representation.WriterRepresentation;
import org.restlet.resource.ServerResource;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

public class ManifestResource extends ServerResource {
    {
//        getVariants().add(new JSONRepresentation());
        getVariants().add(new TextRepresentation());
    }
    
    private class JSONRepresentation extends WriterRepresentation {
        public JSONRepresentation() {
            super(MediaType.APPLICATION_JSON);
        }

        @Override
        public void write(Writer writer) throws IOException {
            JsonObject refs = buildRefJson();
            Gson gson = new Gson();
            gson.toJson(refs, new JsonWriter(writer));
        }
        
        private JsonObject buildRefJson() {
            Form options = getRequest().getResourceRef().getQueryAsForm();
            boolean remotes = Boolean.valueOf(options.getFirstValue("remotes", "false"));
            GeoGIT ggit = (GeoGIT) getApplication().getContext().getAttributes().get("geogit");
            ImmutableList<Ref> refs = ggit.command(BranchListOp.class).setRemotes(remotes).call();
            JsonObject refJson = new JsonObject();
            JsonArray refArray = new JsonArray();
            for (Ref r : refs) {
                JsonObject branch = new JsonObject();
                branch.addProperty("name", r.getName());
                branch.addProperty("objectid", r.getObjectId().toString());
                refArray.add(branch);
            }
            refJson.add("refs", refArray);
            return refJson;
        }
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
