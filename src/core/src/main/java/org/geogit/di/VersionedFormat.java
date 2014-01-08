package org.geogit.di;

import com.google.common.base.Objects;

public final class VersionedFormat {
    private final String version;
    private final String format;

    public VersionedFormat(String format, String version) {
        this.format = format;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof VersionedFormat) {
            VersionedFormat that = (VersionedFormat) o;
            return this.version.equals(that.version) && this.format.equals(that.format);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(version, format);
    }

    @Override
    public String toString() {
        return format + ";v=" + version;
    }
}
