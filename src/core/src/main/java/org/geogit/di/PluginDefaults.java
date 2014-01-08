package org.geogit.di;

public final class PluginDefaults {
    private final VersionedFormat refs, objects, staging, graph;

    public PluginDefaults(VersionedFormat objects, VersionedFormat staging, VersionedFormat refs, VersionedFormat graph) {
        this.refs = refs;
        this.objects = objects;
        this.staging = staging;
        this.graph = graph;
    }

    public VersionedFormat getRefs() {
        return refs;
    }

    public VersionedFormat getObjects() {
        return objects;
    }

    public VersionedFormat getStaging() {
        return staging;
    }

    public VersionedFormat getGraph() {
        return graph;
    }
}
