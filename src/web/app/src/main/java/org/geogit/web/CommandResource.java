package org.geogit.web;

import java.io.IOException;
import java.io.Writer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.codehaus.jettison.mapped.MappedNamespaceConvention;
import org.codehaus.jettison.mapped.MappedXMLStreamWriter;
import org.geogit.api.GeoGIT;
import org.geogit.web.api.CommandBuilder;
import org.geogit.web.api.CommandContext;
import org.geogit.web.api.CommandResponse;
import org.geogit.web.api.CommandSpecException;
import org.geogit.web.api.ParameterSet;
import org.geogit.web.api.ResponseWriter;
import org.geogit.web.api.WebAPICommand;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.Variant;
import org.restlet.representation.WriterRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;

public class CommandResource extends ServerResource {

    @Post("json|xml")
    public Representation postCommand(Variant variant) {
        return runCommand(variant);
    }

    @Get("json|xml")
    public Representation getCommand(Variant variant) {
        return runCommand(variant);
    }

    private Representation runCommand(Variant variant) {
        Representation rep = null;
        WebAPICommand command = null;
        Form options = getRequest().getResourceRef().getQueryAsForm();
        String commandName = (String) getRequest().getAttributes().get("command");
        MediaType format = resolveFormat(options, variant);
        try {
            ParameterSet params = new FormParams(options);
            command = CommandBuilder.build(commandName, params);
            assert command != null;
        } catch (CommandSpecException ex) {
            rep = formatException(ex, format);
        }
        try {
            if (command != null) {
                GeoGIT geogit = (GeoGIT) getApplication().getContext().getAttributes()
                        .get("geogit");
                RestletContext ctx = new RestletContext(geogit);
                command.run(ctx);
                rep = ctx.getRepresentation(format, getJSONPCallback());
            }
        } catch (IllegalArgumentException ex) {
            rep = formatException(ex, format);
        } catch (Exception ex) {
            rep = formatUnexpectedException(ex, format);
        }
        return rep;
    }

    private Representation formatException(IllegalArgumentException ex, MediaType format) {
        Logger logger = getLogger();
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "CommandSpecException", ex);
        }
        return new JettisonRepresentation(format, CommandResponse.error(ex.getMessage()),
                getJSONPCallback());
    }

    private Representation formatUnexpectedException(Exception ex, MediaType format) {
        Logger logger = getLogger();
        UUID uuid = UUID.randomUUID();
        logger.log(Level.SEVERE, "Unexpected exception : " + uuid, ex);
        return new JettisonRepresentation(format, CommandResponse.error("Unexpected exception : "
                + uuid), getJSONPCallback());
    }

    private String getJSONPCallback() {
        Form form = getRequest().getResourceRef().getQueryAsForm();
        return form.getFirstValue("callback", null);
    }

    private MediaType resolveFormat(Form options, Variant variant) {
        MediaType retval = variant.getMediaType();
        String requested = options.getFirstValue("output_format");
        if (requested != null) {
            if (requested.equalsIgnoreCase("xml")) {
                retval = MediaType.APPLICATION_XML;
            } else if (requested.equalsIgnoreCase("json")) {
                retval = MediaType.APPLICATION_JSON;
            } else {
                throw new ResourceException(org.restlet.data.Status.CLIENT_ERROR_BAD_REQUEST,
                        "Invalid output_format '" + requested + "'");
            }
        }
        return retval;
    }

    static class RestletContext implements CommandContext {

        CommandResponse responseContent;

        final GeoGIT geogit;

        RestletContext(GeoGIT geogit) {
            this.geogit = geogit;
        }

        @Override
        public GeoGIT getGeoGIT() {
            return geogit;
        }

        Representation getRepresentation(MediaType format, String callback) {
            return new JettisonRepresentation(format, responseContent, callback);
        }

        @Override
        public void setResponseContent(CommandResponse responseContent) {
            this.responseContent = responseContent;
        }
    }

    static class JettisonRepresentation extends WriterRepresentation {

        final CommandResponse impl;

        String callback;

        public JettisonRepresentation(MediaType mediaType, CommandResponse impl, String callback) {
            super(mediaType);
            this.impl = impl;
            this.callback = callback;
        }

        private XMLStreamWriter createWriter(Writer writer) {
            final MediaType mediaType = getMediaType();
            XMLStreamWriter xml;
            if (mediaType.getSubType().equalsIgnoreCase("xml")) {
                try {
                    xml = XMLOutputFactory.newFactory().createXMLStreamWriter(writer);
                } catch (XMLStreamException ex) {
                    throw new RuntimeException(ex);
                }
                callback = null; // this doesn't make sense
            } else if (mediaType == MediaType.APPLICATION_JSON) {
                xml = new MappedXMLStreamWriter(new MappedNamespaceConvention(), writer);
            } else {
                throw new RuntimeException("mediatype not handled " + mediaType);
            }
            return xml;
        }

        @Override
        public void write(Writer writer) throws IOException {
            XMLStreamWriter stax = null;
            if (callback != null) {
                writer.write(callback);
                writer.write('(');
            }
            try {
                stax = createWriter(writer);
                impl.write(new ResponseWriter(stax));
                stax.flush();
                stax.close();
            } catch (Exception ex) {
                throw new IOException(ex);
            }
            if (callback != null) {
                writer.write(");");
            }
        }
    }
}
