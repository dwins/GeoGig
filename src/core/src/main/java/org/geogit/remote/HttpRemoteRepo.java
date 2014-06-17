/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.remote;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.geogit.api.ObjectId;
import org.geogit.api.Ref;
import org.geogit.api.RevCommit;
import org.geogit.api.RevObject;
import org.geogit.api.RevTag;
import org.geogit.api.porcelain.SynchronizationException;
import org.geogit.remote.BinaryPackedObjects.IngestResults;
import org.geogit.remote.HttpUtils.ReportingOutputStream;
import org.geogit.repository.Repository;
import org.geogit.storage.DeduplicationService;
import org.geogit.storage.Deduplicator;
import org.geogit.storage.ObjectDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * An implementation of a remote repository that exists on a remote machine and made public via an
 * http interface.
 * 
 * @see AbstractRemoteRepo
 */
class HttpRemoteRepo extends AbstractRemoteRepo {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpRemoteRepo.class);

    private URL repositoryURL;

    final private DeduplicationService deduplicationService;

    /**
     * Constructs a new {@code HttpRemoteRepo} with the given parameters.
     * 
     * @param repositoryURL the url of the remote repository
     */
    public HttpRemoteRepo(URL repositoryURL, Repository localRepository,
            DeduplicationService deduplicationService) {
        super(localRepository);
        this.deduplicationService = deduplicationService;
        String url = repositoryURL.toString();
        if (url.endsWith("/")) {
            url = url.substring(0, url.lastIndexOf('/'));
        }
        try {
            this.repositoryURL = new URL(url);
        } catch (MalformedURLException e) {
            this.repositoryURL = repositoryURL;
        }
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
            connection = HttpUtils.connect(expanded);

            // Get Response
            InputStream is = HttpUtils.getResponseStream(connection);
            try {
                BufferedReader rd = new BufferedReader(new InputStreamReader(is));
                String line;

                while ((line = rd.readLine()) != null) {
                    if (line.startsWith("HEAD")) {
                        headRef = HttpUtils.parseRef(line);
                    }
                }
                rd.close();
            } finally {
                is.close();
            }

        } catch (Exception e) {

            Throwables.propagate(e);

        } finally {
            HttpUtils.consumeErrStreamAndCloseConnection(connection);
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
            connection = HttpUtils.connect(expanded);

            // Get Response
            InputStream is = HttpUtils.getResponseStream(connection);
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            String line;
            try {
                while ((line = rd.readLine()) != null) {
                    if ((getHeads && line.startsWith("refs/heads"))
                            || (getTags && line.startsWith("refs/tags"))) {
                        builder.add(HttpUtils.parseRef(line));
                    }
                }
            } finally {
                rd.close();
            }

        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            HttpUtils.consumeErrStreamAndCloseConnection(connection);
        }
        return builder.build();
    }

    /**
     * Fetch all new objects from the specified {@link Ref} from the remote.
     * 
     * @param ref the remote ref that points to new commit data
     * @param fetchLimit the maximum depth to fetch
     */
    @Override
    public void fetchNewData(Ref ref, Optional<Integer> fetchLimit) {

        CommitTraverser traverser = getFetchTraverser(fetchLimit);

        try {
            traverser.traverse(ref.getObjectId());
            List<ObjectId> want = new LinkedList<ObjectId>();
            want.addAll(traverser.commits);
            Collections.reverse(want);
            Set<ObjectId> have = new HashSet<ObjectId>();
            have.addAll(traverser.have);
            while (!want.isEmpty()) {
                fetchMoreData(want, have);
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    /**
     * Push all new objects from the specified {@link Ref} to the remote.
     * 
     * @param ref the local ref that points to new commit data
     * @param refspec the remote branch to push to
     */
    @Override
    public void pushNewData(Ref ref, String refspec) throws SynchronizationException {
        Optional<Ref> remoteRef = HttpUtils.getRemoteRef(repositoryURL, refspec);
        checkPush(ref, remoteRef);
        beginPush();

        CommitTraverser traverser = getPushTraverser(remoteRef);

        traverser.traverse(ref.getObjectId());

        List<ObjectId> toSend = new LinkedList<ObjectId>();
        toSend.addAll(traverser.commits);
        Collections.reverse(toSend);
        Set<ObjectId> have = new HashSet<ObjectId>();
        have.addAll(traverser.have);

        Deduplicator deduplicator = deduplicationService.createDeduplicator();
        try {
            sendPackedObjects(toSend, have, deduplicator);
        } finally {
            deduplicator.release();
        }

        ObjectId originalRemoteRefValue = ObjectId.NULL;
        if (remoteRef.isPresent()) {
            originalRemoteRefValue = remoteRef.get().getObjectId();
        }
        endPush(refspec, ref.getObjectId(), originalRemoteRefValue.toString());
    }

    private void sendPackedObjects(final List<ObjectId> toSend, final Set<ObjectId> roots,
            Deduplicator deduplicator) {
        Set<ObjectId> sent = new HashSet<ObjectId>();
        while (!toSend.isEmpty()) {
            try {
                BinaryPackedObjects.Callback callback = new BinaryPackedObjects.Callback() {
                    @Override
                    public void callback(Supplier<RevObject> supplier) {
                        RevObject object = supplier.get();
                        if (object instanceof RevCommit) {
                            RevCommit commit = (RevCommit) object;
                            toSend.remove(commit.getId());
                            roots.removeAll(commit.getParentIds());
                            roots.add(commit.getId());
                        }
                    }
                };
                ObjectDatabase database = localRepository.objectDatabase();
                BinaryPackedObjects packer = new BinaryPackedObjects(database);

                ImmutableList<ObjectId> have = ImmutableList.copyOf(roots);
                final boolean traverseCommits = false;

                Supplier<ReportingOutputStream> outputSupplier = getMemoizedOutputSupplier();
                Stopwatch sw = Stopwatch.createStarted();
                long writtenObjectsCount = packer.write(outputSupplier, toSend, have, sent,
                        callback, traverseCommits, deduplicator);
                sw.stop();
                ReportingOutputStream out = outputSupplier.get();
                out.flush();
                out.close();

                LOGGER.info(String.format("HttpRemoteRepo: Written %,d objects.\n"
                        + "Time to process: %s.\n"
                        + "Compressed size: %,d bytes.\nUncompressed size: %,d bytes.\n",
                        writtenObjectsCount, sw, out.compressedSize(), out.unCompressedSize()));
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
    }

    private Supplier<ReportingOutputStream> getMemoizedOutputSupplier() {
        Supplier<ReportingOutputStream> outputSupplier = Suppliers
                .memoize(new Supplier<ReportingOutputStream>() {

                    @Override
                    public ReportingOutputStream get() {
                        String expanded = repositoryURL.toString() + "/repo/sendobject";
                        System.err.println("connecting to " + expanded);
                        try {
                            HttpURLConnection connection = (HttpURLConnection) new URL(expanded)
                                    .openConnection();
                            connection.setDoOutput(true);
                            connection.setDoInput(false);
                            connection.setUseCaches(false);
                            connection.setRequestMethod("POST");
                            connection.setChunkedStreamingMode(4096);
                            connection.setRequestProperty("content-length", "-1");
                            connection.setRequestProperty("content-encoding", "gzip");
                            OutputStream out = connection.getOutputStream();
                            ReportingOutputStream rout = HttpUtils.newReportingOutputStream(out,
                                    true);
                            System.err.println("Connected.");
                            return rout;
                        } catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
        return outputSupplier;
    }

    /**
     * Delete a {@link Ref} from the remote repository.
     * 
     * @param refspec the ref to delete
     */
    @Override
    public void deleteRef(String refspec) {
        HttpUtils.updateRemoteRef(repositoryURL, refspec, null, true);
    }

    private void beginPush() {
        HttpUtils.beginPush(repositoryURL);
    }

    private void endPush(String refspec, ObjectId newCommitId, String originalRefValue) {
        HttpUtils.endPush(repositoryURL, refspec, newCommitId, originalRefValue);
    }

    /**
     * Retrieve objects from the remote repository, and update have/want lists accordingly.
     * Specifically, any retrieved commits are removed from the want list and added to the have
     * list, and any parents of those commits are removed from the have list (it only represents the
     * most recent common commits.) Retrieved objects are added to the local repository, and the
     * want/have lists are updated in-place.
     * 
     * @param want a list of ObjectIds that need to be fetched
     * @param have a list of ObjectIds that are in common with the remote repository
     */
    private void fetchMoreData(final List<ObjectId> want, final Set<ObjectId> have) {
        final JsonObject message = createFetchMessage(want, have);
        final URL resourceURL;
        try {
            resourceURL = new URL(repositoryURL.toString() + "/repo/batchobjects");
        } catch (MalformedURLException e) {
            throw Throwables.propagate(e);
        }

        System.err.println("Fetching from " + resourceURL.toExternalForm());
        final HttpURLConnection connection;
        try {
            final Gson gson = new Gson();
            OutputStream out;
            final Writer writer;
            connection = (HttpURLConnection) resourceURL.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.addRequestProperty("Accept-Encoding", "gzip");
            out = connection.getOutputStream();
            writer = new OutputStreamWriter(out);
            gson.toJson(message, writer);
            writer.flush();
            out.flush();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        System.err.println("Request sent. Waiting for response...");
        final HttpUtils.ReportingInputStream in = HttpUtils.getResponseStream(connection);
        System.err.println("Processing response...");

        BinaryPackedObjects unpacker = new BinaryPackedObjects(localRepository.objectDatabase());
        BinaryPackedObjects.Callback callback = new BinaryPackedObjects.Callback() {
            @Override
            public void callback(Supplier<RevObject> supplier) {
                RevObject object = supplier.get();
                if (object instanceof RevCommit) {
                    RevCommit commit = (RevCommit) object;
                    want.remove(commit.getId());
                    have.removeAll(commit.getParentIds());
                    have.add(commit.getId());
                } else if (object instanceof RevTag) {
                    RevTag tag = (RevTag) object;
                    want.remove(tag.getId());
                    have.remove(tag.getCommitId());
                    have.add(tag.getId());
                }
            }
        };

        Stopwatch sw = Stopwatch.createStarted();
        IngestResults ingestResults = unpacker.ingest(in, callback);
        sw.stop();

        System.err
                .printf("Processed %,d objects.\nInserted: %,d.\nExisting: %,d.\nTime to process: %s.\nCompressed size: %,d bytes.\nUncompressed size: %,d bytes.\n",
                        ingestResults.total(), ingestResults.getInserted(),
                        ingestResults.getExisting(), sw, in.compressedSize(), in.unCompressedSize());
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

    /**
     * @return the {@link RepositoryWrapper} for this remote
     */
    @Override
    public RepositoryWrapper getRemoteWrapper() {
        return new HttpRepositoryWrapper(repositoryURL);
    }

    /**
     * Gets the depth of the remote repository.
     * 
     * @return the depth of the repository, or {@link Optional#absent()} if the repository is not
     *         shallow
     */
    @Override
    public Optional<Integer> getDepth() {
        return HttpUtils.getDepth(repositoryURL, null);
    }
}
