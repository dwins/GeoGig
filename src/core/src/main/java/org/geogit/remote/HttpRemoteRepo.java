package org.geogit.remote;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.geogit.api.Bucket;
import org.geogit.api.CommitBuilder;
import org.geogit.api.GeoGIT;
import org.geogit.api.Node;
import org.geogit.api.NodeRef;
import org.geogit.api.ObjectId;
import org.geogit.api.Ref;
import org.geogit.api.RevCommit;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureBuilder;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevObject;
import org.geogit.api.RevObject.TYPE;
import org.geogit.api.RevTree;
import org.geogit.api.RevTreeBuilder;
import org.geogit.api.SymRef;
import org.geogit.api.plumbing.DiffTree;
import org.geogit.api.plumbing.FindCommonAncestor;
import org.geogit.api.plumbing.RevObjectParse;
import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.api.porcelain.PushException;
import org.geogit.api.porcelain.PushException.StatusCode;
import org.geogit.repository.Repository;

import com.google.common.base.Optional;
import org.geogit.repository.Repository;
import org.geogit.storage.ObjectReader;
import org.geogit.storage.datastream.CommitReader;
import org.geogit.storage.datastream.DataStreamSerializationFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * An implementation of a remote repository that exists on a remote machine and made public via an
 * http interface.
 * 
 * @see IRemoteRepo
 */
public class HttpRemoteRepo implements IRemoteRepo {

    private URL repositoryURL;

    private Queue<ObjectId> commitQueue;

    private List<ObjectId> fetchedIds;

    /**
     * Constructs a new {@code HttpRemoteRepo} with the given parameters.
     * 
     * @param repositoryURL the url of the remote repository
     */
    public HttpRemoteRepo(URL repositoryURL) {
        String url = repositoryURL.toString();
        if (url.endsWith("/")) {
            url = url.substring(0, url.lastIndexOf('/'));
        }
        try {
            this.repositoryURL = new URL(url);
        } catch (MalformedURLException e) {
            this.repositoryURL = repositoryURL;
        }
        commitQueue = new LinkedList<ObjectId>();
    }

    /**
     * Currently does nothing for HTTP Remote.
     * 
     * @throws IOException
     */
    @Override
    public void open() throws IOException {

    }

    /**
     * Currently does nothing for HTTP Remote.
     * 
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }

    /**
     * @return the remote's HEAD {@link Ref}.
     */
    @Override
    public Ref headRef() {
        HttpURLConnection connection = null;
        Ref headRef = null;
        try {
            String expanded = repositoryURL.toString() + "/repo/manifest";

            connection = (HttpURLConnection) new URL(expanded).openConnection();
            connection.setRequestMethod("GET");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            // Get Response
            InputStream is = connection.getInputStream();
            try {
                BufferedReader rd = new BufferedReader(new InputStreamReader(is));
                String line;

                while ((line = rd.readLine()) != null) {
                    if (line.startsWith("HEAD")) {
                        headRef = parseRef(line);
                    }
                }
                rd.close();
            } finally {
                is.close();
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            consumeErrStreamAndCloseConnection(connection);
        }
        return headRef;
    }

    /**
     * List the remote's {@link Ref refs}.
     * 
     * @param getHeads whether to return refs in the {@code refs/heads} namespace
     * @param getTags whether to return refs in the {@code refs/tags} namespace
     * @return an immutable set of refs from the remote
     */
    @Override
    public ImmutableSet<Ref> listRefs(final boolean getHeads, final boolean getTags) {
        HttpURLConnection connection = null;
        ImmutableSet.Builder<Ref> builder = new ImmutableSet.Builder<Ref>();
        try {
            String expanded = repositoryURL.toString() + "/repo/manifest";

            connection = (HttpURLConnection) new URL(expanded).openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            // Get Response
            InputStream is = connection.getInputStream();

            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            String line;
            try {
                while ((line = rd.readLine()) != null) {
                    if ((getHeads && line.startsWith("refs/heads"))
                            || (getTags && line.startsWith("refs/tags"))) {
                        builder.add(parseRef(line));
                    }
                }
            } finally {
                rd.close();
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            consumeErrStreamAndCloseConnection(connection);
        }
        return builder.build();
    }

    /**
     * @param connection
     */
    private void consumeErrStreamAndCloseConnection(@Nullable HttpURLConnection connection) {
        if (connection == null) {
            return;
        }
        try {
            InputStream es = ((HttpURLConnection) connection).getErrorStream();
            consumeAndCloseStream(es);
        } catch (IOException ex) {
            throw Throwables.propagate(ex);
        } finally {
            connection.disconnect();
        }
    }

    private void consumeAndCloseStream(InputStream stream) throws IOException {
        if (stream != null) {
            try {
                // read the response body
                while (stream.read() > -1) {
                    ;
                }
            } finally {
                // close the errorstream
                Closeables.closeQuietly(stream);
            }
        }
    }

    private Ref parseRef(String refString) {
        Ref ref = null;
        String[] tokens = refString.split(" ");
        if (tokens.length == 2) {
            // normal ref
            // NAME HASH
            String name = tokens[0];
            ObjectId objectId = ObjectId.valueOf(tokens[1]);
            ref = new Ref(name, objectId, RevObject.TYPE.COMMIT);
        } else {
            // symbolic ref
            // NAME TARGET HASH
            String name = tokens[0];
            String targetRef = tokens[1];
            ObjectId targetObjectId = ObjectId.valueOf(tokens[2]);
            Ref target = new Ref(targetRef, targetObjectId, RevObject.TYPE.COMMIT);
            ref = new SymRef(name, target);

        }
        return ref;
    }

    /**
     * Fetch all new objects from the specified {@link Ref} from the remote.
     * 
     * @param localRepository the repository to add new objects to
     * @param ref the remote ref that points to new commit data
     */
    @Override
    public void fetchNewData(Repository localRepository, Ref ref) {
        final List<ObjectId> want = new ArrayList<ObjectId>();
        if (!localRepository.blobExists(ref.getObjectId())) { 
            want.add(ref.getObjectId());
        }
        final Set<ObjectId> have = commonRoots(want, localRepository);
        while (! want.isEmpty()) {
            // fetchMoreData retrieves objects from the remote repo and
            // updates the want/have lists accordingly - retrieved commits
            // are removed from the want list and added to the have list.  
            // Additionally, parents of retrieved commits are removed from
            // the have list (since it represents the latest common commits.)
            fetchMoreData(localRepository, want, have);
        }
        
    }
    
    /**
     * Retrieve objects from the remote repository, and update have/want lists accordingly.
     * Specifically, any retrieved commits are removed from the want list and added to the have
     * list, and any parents of those commits are removed from the have list (it only represents
     * the most recent common commits.)  Retrieved objects are added to the local repository, and
     * the want/have lists are updated in-place.
     * 
     * @param localRepository the local geogit Repository
     * @param want a list of ObjectIds that need to be fetched
     * @param have a list of ObjectIds that are in common with the remote repository
     */
    private void fetchMoreData(
            final Repository localRepository,
            final List<ObjectId> want,
            final Set<ObjectId> have)
    {
        final JsonObject message = createFetchMessage(want, have);
        System.out.println("Fetch: " + message);
        final URL resourceURL;
        try {
            resourceURL = new URL(repositoryURL.toString() + "/repo/objects");
        } catch (MalformedURLException e) {
            throw Throwables.propagate(e);
        }
        
        final Gson gson = new Gson();
        final HttpURLConnection connection;
        final OutputStream out;
        final Writer writer;
        try {
            connection = (HttpURLConnection) resourceURL.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            out = connection.getOutputStream();
            writer = new OutputStreamWriter(out);
            gson.toJson(message, writer);
            writer.flush();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        final InputStream in;
        final PushbackInputStream pushback;
        try {
            in = connection.getInputStream();
            pushback = new PushbackInputStream(in);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        
        DataStreamSerializationFactory factory = new DataStreamSerializationFactory();
        ObjectReader<RevObject> objectReader = factory.createObjectReader();
        while (true) {
            final ObjectId id;
            try {
                byte[] bytes = readNBytes(pushback, 20);
                id = new ObjectId(bytes);
            } catch (EOFException e) {
                break;
            } catch (IndexOutOfBoundsException e) {
                break;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            
            RevObject object = objectReader.read(id, pushback);
            localRepository.getObjectDatabase().put(object);
            if (object instanceof RevCommit) {
                RevCommit commit = (RevCommit) object;
                want.remove(id);
                have.removeAll(commit.getParentIds());
                have.add(id);
            }
        }
    }
    
    private byte[] readNBytes(InputStream in, int n) throws IOException {
        byte[] bytes = new byte[n];
        int offset = 0;
        int len = 0;
        while ((len = in.read(bytes, offset, n - offset)) != (n - offset)) {
            offset += len;
        }
        return bytes;
    }
    
    private JsonObject createFetchMessage(List<ObjectId> want, Set<ObjectId> have) {
        JsonObject message = new JsonObject();
        JsonArray wantArray = new JsonArray();
        for (ObjectId id : want) {
            wantArray.add(new JsonPrimitive(id.toString()));
        }
        JsonArray haveArray = new JsonArray();
        for (ObjectId id : have) {
            haveArray.add(new JsonPrimitive(id.toString()));
        }
        message.add("want", wantArray);
        message.add("have", haveArray);
        return message;
    }
    
    public Set<ObjectId> commonRoots(List<ObjectId> want, Repository localRepository) {
        Set<ObjectId> roots = heads(localRepository);
        List<ObjectId> requirements = new ArrayList<ObjectId>(want);
        while (!requirements.isEmpty()) {
            Map<ObjectId, List<ObjectId>> remoteHistory = fetchHistoryFrom(requirements, roots);
            List<ObjectId> nextRequirements = new ArrayList<ObjectId>();
            List<ObjectId> toCheck = new ArrayList<ObjectId>();
            for (ObjectId req : requirements) {
                if (! remoteHistory.containsKey(req)) {
                    throw new IllegalStateException("Want list contains an element that is not on the remote server [" + req + "]; of " + remoteHistory.keySet());
                } else {
                    toCheck.add(req);
                }
            }
            while (!toCheck.isEmpty()) {
                ObjectId possibleRoot = toCheck.remove(0);
                if (localRepository.commitExists(possibleRoot)) {
                    roots.add(possibleRoot);
                } else {
                    if (remoteHistory.containsKey(possibleRoot)) {
                        toCheck.addAll(remoteHistory.get(possibleRoot));
                    } else {
                        nextRequirements.add(possibleRoot);
                    }
                }
            }
            requirements = nextRequirements;
        }
        return roots;
    }
    
    private Set<ObjectId> heads(Repository localRepository) {
        Set<ObjectId> results = new HashSet<ObjectId>();
        Map<String, String> allRefs = localRepository.getRefDatabase().getAll();
        System.out.println("Refs: " + allRefs);
        for (Map.Entry<String, String> entry : allRefs.entrySet()) {
            final String id  = entry.getValue();
            if (id == null) continue;
            if ("0000000000000000000000000000000000000000".equals(id)) continue;
            if (id.startsWith("ref:")) continue;
            results.add(ObjectId.valueOf(entry.getValue()));
        }
        return results;
    }
    
    private Map<ObjectId, List<ObjectId>> fetchHistoryFrom(List<ObjectId> requirements, Set<ObjectId> have) {
        JsonObject message = new JsonObject();
        JsonArray wantArray = new JsonArray();
        for (ObjectId req : requirements) {
            wantArray.add(new JsonPrimitive(req.toString()));
        }
        JsonArray haveArray = new JsonArray();
        for (ObjectId h : have) {
            haveArray.add(new JsonPrimitive(h.toString()));
        }
        message.add("want", wantArray);
        message.add("have", haveArray);
        System.out.println("Exists: " + message.toString());
        
        try {
            final URL resourceURL = new URL(repositoryURL.toString() + "/repo/exists");
            
            final HttpURLConnection conn = (HttpURLConnection) resourceURL.openConnection();
            conn.setRequestMethod("POST");
            conn.setUseCaches(false);
            conn.setDoInput(true);
            conn.setDoOutput(true);
            
            final OutputStream out = conn.getOutputStream();
            final Writer writer = new OutputStreamWriter(out);
            final Gson gson = new Gson();
            gson.toJson(message, writer);
            writer.flush();
            
            final InputStream in = conn.getInputStream();
            final Reader reader = new InputStreamReader(in);
            final JsonParser parser = new JsonParser();
            JsonElement responseJson = parser.parse(reader);
            JsonObject response = responseJson.getAsJsonObject();
            JsonArray responseHaveArray = response.get("history").getAsJsonArray();
            Map<ObjectId, List<ObjectId>> results = new HashMap<ObjectId, List<ObjectId>>();
            for (JsonElement e : responseHaveArray) {
                JsonObject entry = e.getAsJsonObject();
                ObjectId id = ObjectId.valueOf(entry.get("id").getAsJsonPrimitive().getAsString());
                List<ObjectId> parents = new ArrayList<ObjectId>();
                for (JsonElement p : entry.get("parents").getAsJsonArray()) {
                    parents.add(ObjectId.valueOf(p.getAsJsonPrimitive().getAsString()));
                }
                results.put(id, parents);
            }
            return results;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
    
    private <T> T todo() {
        throw new RuntimeException("An implementation is missing");
    }
    
    /**
     * Push all new objects from the specified {@link Ref} to the remote.
     * 
     * @param localRepository the repository to get new objects from
     * @param ref the local ref that points to new commit data
     */
    @Override
    public void pushNewData(Repository localRepository, Ref ref) throws PushException {
        pushNewData(localRepository, ref, ref.getName());
    }

    /**
     * Push all new objects from the specified {@link Ref} to the remote.
     * 
     * @param localRepository the repository to get new objects from
     * @param ref the local ref that points to new commit data
     * @param refspec the remote branch to push to
     */
    @Override
    public void pushNewData(Repository localRepository, Ref ref, String refspec)
            throws PushException {
        Optional<Ref> remoteRef = checkPush(localRepository, ref, refspec);
        beginPush();
        commitQueue.clear();
        commitQueue.add(ref.getObjectId());
        while (!commitQueue.isEmpty()) {
            ObjectId commitId = commitQueue.remove();
            if (walkCommit(commitId, localRepository, true)) {
                RevCommit oldCommit = localRepository.getCommit(commitId);
                ObjectId parentId = oldCommit.getParentIds().get(0);
                RevCommit parentCommit = localRepository.getCommit(parentId);
                Iterator<DiffEntry> diff = localRepository.command(DiffTree.class)
                        .setOldTree(parentCommit.getId()).setNewTree(oldCommit.getId()).call();
                // Send the features that changed.
                while (diff.hasNext()) {
                    DiffEntry entry = diff.next();
                    if (entry.getNewObject() != null) {
                        NodeRef nodeRef = entry.getNewObject();
                        moveObject(nodeRef.getNode().getObjectId(), localRepository, true);
                        ObjectId metadataId = nodeRef.getMetadataId();
                        if (!metadataId.isNull()) {
                            moveObject(metadataId, localRepository, true);
                        }
                    }
                }
            }
        }
        ObjectId originalRemoteRefValue = ObjectId.NULL;
        if (remoteRef.isPresent()) {
            originalRemoteRefValue = remoteRef.get().getObjectId();
        }
        endPush(refspec, ref.getObjectId().toString(), originalRemoteRefValue.toString());
    }

    private Optional<Ref> checkPush(Repository localRepository, Ref ref, String refspec)
            throws PushException {
        Optional<Ref> remoteRef = getRemoteRef(refspec);
        if (remoteRef.isPresent()) {
            if (remoteRef.get().getObjectId().equals(ref.getObjectId())) {
                // The branches are equal, no need to push.
                throw new PushException(StatusCode.NOTHING_TO_PUSH);
            } else if (localRepository.blobExists(remoteRef.get().getObjectId())) {
                RevCommit leftCommit = localRepository.getCommit(remoteRef.get().getObjectId());
                RevCommit rightCommit = localRepository.getCommit(ref.getObjectId());
                Optional<RevCommit> ancestor = localRepository.command(FindCommonAncestor.class)
                        .setLeft(leftCommit).setRight(rightCommit).call();
                if (!ancestor.isPresent()) {
                    // There is no common ancestor, a push will overwrite history
                    throw new PushException(StatusCode.REMOTE_HAS_CHANGES);
                } else if (ancestor.get().getId().equals(ref.getObjectId())) {
                    // My last commit is the common ancestor, the remote already has my data.
                    throw new PushException(StatusCode.NOTHING_TO_PUSH);
                } else if (!ancestor.get().getId().equals(remoteRef.get().getObjectId())) {
                    // The remote branch's latest commit is not my ancestor, a push will cause a
                    // loss of history.
                    throw new PushException(StatusCode.REMOTE_HAS_CHANGES);
                }
            } else {
                // The remote has data that I do not, a push will cause this data to be lost.
                throw new PushException(StatusCode.REMOTE_HAS_CHANGES);
            }
        }
        return remoteRef;
    }

    /**
     * Delete a {@link Ref} from the remote repository.
     * 
     * @param refspec the ref to delete
     */
    @Override
    public void deleteRef(String refspec) {
        updateRemoteRef(refspec, null, true);
    }

    private void beginPush() {
        HttpURLConnection connection = null;
        try {
            String internalIp = InetAddress.getLocalHost().getHostName();
            String expanded = repositoryURL.toString() + "/repo/beginpush?internalIp=" + internalIp;

            connection = (HttpURLConnection) new URL(expanded).openConnection();
            connection.setRequestMethod("GET");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            InputStream stream = connection.getInputStream();
            consumeAndCloseStream(stream);

        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            consumeErrStreamAndCloseConnection(connection);
        }
    }

    private void endPush(String refspec, String oid, String originalRefValue) {
        HttpURLConnection connection = null;
        try {
            String internalIp = InetAddress.getLocalHost().getHostName();
            String expanded = repositoryURL.toString() + "/repo/endpush?refspec=" + refspec
                    + "&objectId=" + oid + "&internalIp=" + internalIp + "&originalRefValue="
                    + originalRefValue;

            connection = (HttpURLConnection) new URL(expanded).openConnection();
            connection.setRequestMethod("GET");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            connection.getInputStream();

        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            consumeErrStreamAndCloseConnection(connection);
        }
    }

    private Optional<Ref> getRemoteRef(String refspec) {
        HttpURLConnection connection = null;
        Optional<Ref> remoteRef = Optional.absent();
        try {
            String expanded = repositoryURL.toString() + "/refparse?name=" + refspec;

            connection = (HttpURLConnection) new URL(expanded).openConnection();
            connection.setRequestMethod("GET");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            InputStream inputStream = connection.getInputStream();

            XMLStreamReader reader = XMLInputFactory.newFactory()
                    .createXMLStreamReader(inputStream);

            try {
                readToElementStart(reader, "Ref");
                if (reader.hasNext()) {

                    readToElementStart(reader, "name");
                    final String refName = reader.getElementText();

                    readToElementStart(reader, "objectId");
                    final String objectId = reader.getElementText();

                    readToElementStart(reader, "target");
                    String target = null;
                    if (reader.hasNext()) {
                        target = reader.getElementText();
                    }
                    reader.close();

                    if (target != null) {
                        remoteRef = Optional.of((Ref) new SymRef(refName, new Ref(target, ObjectId
                                .valueOf(objectId), RevObject.TYPE.COMMIT)));
                    } else {
                        remoteRef = Optional.of(new Ref(refName, ObjectId.valueOf(objectId),
                                RevObject.TYPE.COMMIT));
                    }
                }

            } finally {
                reader.close();
                inputStream.close();
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            consumeErrStreamAndCloseConnection(connection);
        }
        return remoteRef;
    }

    private Ref updateRemoteRef(String refspec, ObjectId newValue, boolean delete) {
        HttpURLConnection connection = null;
        Ref updatedRef = null;
        try {
            String expanded;
            if (!delete) {
                expanded = repositoryURL.toString() + "/updateref?name=" + refspec + "&newValue="
                        + newValue.toString();
            } else {
                expanded = repositoryURL.toString() + "/updateref?name=" + refspec + "&delete=true";
            }

            connection = (HttpURLConnection) new URL(expanded).openConnection();
            connection.setRequestMethod("GET");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            InputStream inputStream = connection.getInputStream();

            XMLStreamReader reader = XMLInputFactory.newFactory()
                    .createXMLStreamReader(inputStream);

            try {
                readToElementStart(reader, "ChangedRef");

                readToElementStart(reader, "name");
                final String refName = reader.getElementText();

                readToElementStart(reader, "objectId");
                final String objectId = reader.getElementText();

                readToElementStart(reader, "target");
                String target = null;
                if (reader.hasNext()) {
                    target = reader.getElementText();
                }
                reader.close();

                if (target != null) {
                    updatedRef = new SymRef(refName, new Ref(target, ObjectId.valueOf(objectId),
                            RevObject.TYPE.COMMIT));
                } else {
                    updatedRef = new Ref(refName, ObjectId.valueOf(objectId), RevObject.TYPE.COMMIT);
                }

            } finally {
                reader.close();
                inputStream.close();
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            consumeErrStreamAndCloseConnection(connection);
        }
        return updatedRef;
    }

    private void readToElementStart(XMLStreamReader reader, String name) throws XMLStreamException {
        while (reader.hasNext()) {
            if (reader.isStartElement() && reader.getLocalName().equals(name)) {
                break;
            }
            reader.next();
        }
    }

    private boolean walkCommit(ObjectId commitId, Repository localRepo, boolean sendObject) {
        // See if we already have it
        if (sendObject) {
            if (networkObjectExists(commitId, localRepo)) {
                return false;
            }
        } else if (localRepo.getObjectDatabase().exists(commitId)) {
            return false;
        }

        Optional<RevObject> object = sendObject ? sendNetworkObject(commitId, localRepo)
                : getNetworkObject(commitId, localRepo);
        if (object.isPresent() && object.get().getType().equals(TYPE.COMMIT)) {
            RevCommit commit = (RevCommit) object.get();
            walkTree(commit.getTreeId(), localRepo, sendObject);

            for (ObjectId parentCommit : commit.getParentIds()) {
                commitQueue.add(parentCommit);
            }
        }
        return true;
    }

    private void walkTree(ObjectId treeId, Repository localRepo, boolean sendObject) {
        // See if we already have it
        if (sendObject) {
            if (networkObjectExists(treeId, localRepo)) {
                return;
            }
        } else if (localRepo.getObjectDatabase().exists(treeId)) {
            return;
        }

        Optional<RevObject> object = sendObject ? sendNetworkObject(treeId, localRepo)
                : getNetworkObject(treeId, localRepo);
        if (object.isPresent() && object.get().getType().equals(TYPE.TREE)) {
            RevTree tree = (RevTree) object.get();

            walkLocalTree(tree, localRepo, sendObject);
        }
    }

    private void walkLocalTree(RevTree tree, Repository localRepo, boolean sendObject) {
        // walk subtrees
        if (tree.buckets().isPresent()) {
            for (Bucket bucket : tree.buckets().get().values()) {
                ObjectId bucketId = bucket.id();
                walkTree(bucketId, localRepo, sendObject);
            }
        } else {
            // get new objects
            for (Iterator<Node> children = tree.children(); children.hasNext();) {
                Node ref = children.next();
                if (ref.getType() == RevObject.TYPE.TREE || !sendObject) {
                    moveObject(ref.getObjectId(), localRepo, sendObject);
                    ObjectId metadataId = ref.getMetadataId().or(ObjectId.NULL);
                    if (!metadataId.isNull()) {
                        moveObject(metadataId, localRepo, sendObject);
                    }
                }
            }
        }
    }

    private void moveObject(ObjectId objectId, Repository localRepo, boolean sendObject) {
        // See if we already have it
        if (sendObject) {
            if (networkObjectExists(objectId, localRepo)) {
                return;
            }
        } else if (localRepo.getObjectDatabase().exists(objectId)) {
            return;
        }

        Optional<RevObject> childObject = sendObject ? sendNetworkObject(objectId, localRepo)
                : getNetworkObject(objectId, localRepo);
        if (childObject.isPresent()) {
            RevObject revObject = childObject.get();
            if (TYPE.TREE.equals(revObject.getType())) {
                walkLocalTree((RevTree) revObject, localRepo, sendObject);
            }
        }
    }

    private boolean networkObjectExists(ObjectId objectId, Repository localRepo) {
        HttpURLConnection connection = null;
        boolean exists = false;
        try {
            String internalIp = InetAddress.getLocalHost().getHostName();
            String expanded = repositoryURL.toString() + "/repo/exists?oid=" + objectId.toString()
                    + "&internalIp=" + internalIp;

            connection = (HttpURLConnection) new URL(expanded).openConnection();
            connection.setRequestMethod("GET");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            // Get Response
            InputStream is = connection.getInputStream();
            try {
                BufferedReader rd = new BufferedReader(new InputStreamReader(is));
                String line = rd.readLine();

                exists = line.startsWith("1");
            } finally {
                consumeAndCloseStream(is);
            }

        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            consumeErrStreamAndCloseConnection(connection);
        }
        return exists;
    }

    private Optional<RevObject> getNetworkObject(ObjectId objectId, Repository localRepo) {
        HttpURLConnection connection = null;
        try {
            String expanded = repositoryURL.toString() + "/repo/objects/" + objectId.toString();
            connection = (HttpURLConnection) new URL(expanded).openConnection();
            connection.setRequestMethod("GET");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            // Get Response
            InputStream is = connection.getInputStream();
            try {
                localRepo.getObjectDatabase().put(objectId, is);
            } finally {
                consumeAndCloseStream(is);
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            consumeErrStreamAndCloseConnection(connection);
        }

        return localRepo.command(RevObjectParse.class).setObjectId(objectId).call();

    }

    private Optional<RevObject> sendNetworkObject(ObjectId objectId, Repository localRepo) {
        Optional<RevObject> object = localRepo.command(RevObjectParse.class).setObjectId(objectId)
                .call();

        HttpURLConnection connection = null;
        try {
            String internalIp = InetAddress.getLocalHost().getHostName();
            String expanded = repositoryURL.toString() + "/repo/sendobject?internalIp="
                    + internalIp;
            connection = (HttpURLConnection) new URL(expanded).openConnection();
            connection.setRequestMethod("POST");

            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(true);

            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            try {
                wr.write(objectId.getRawValue());
                InputStream rawObject = localRepo.getIndex().getDatabase().getRaw(objectId);
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = rawObject.read(buffer)) != -1) {
                    wr.write(buffer, 0, bytesRead);
                }
                wr.flush();
            } finally {
                wr.close();
            }

            // Get Response
            InputStream is = connection.getInputStream();
            try {
                BufferedReader rd = new BufferedReader(new InputStreamReader(is));
                String line;

                while ((line = rd.readLine()) != null) {
                    if (line.contains("Object already existed")) {
                        return Optional.absent();
                    }
                }
                rd.close();
            } finally {
                consumeAndCloseStream(is);
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            consumeErrStreamAndCloseConnection(connection);
        }
        return object;
    }
}
