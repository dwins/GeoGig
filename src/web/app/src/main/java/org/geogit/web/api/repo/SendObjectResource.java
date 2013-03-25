package org.geogit.web.api.repo;

import java.io.IOException;
import java.io.InputStream;

import org.geogit.api.GeoGIT;
import org.geogit.remote.BinaryPackedObjects;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

public class SendObjectResource extends ServerResource {
    @Post
    public Representation acceptObject(Representation entity) throws IOException {
        final InputStream input = entity.getStream();
        final GeoGIT ggit = (GeoGIT) getApplication().getContext().getAttributes().get("geogit");
        final BinaryPackedObjects unpacker = new BinaryPackedObjects(ggit.getRepository());
        
        unpacker.ingest(input);
        return new StringRepresentation("Ingested", MediaType.TEXT_PLAIN);
    }
}
