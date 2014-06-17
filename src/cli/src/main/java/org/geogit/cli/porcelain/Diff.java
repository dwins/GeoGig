/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.cli.porcelain;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import jline.console.ConsoleReader;

import org.fusesource.jansi.Ansi;
import org.geogit.api.GeoGIT;
import org.geogit.api.Ref;
import org.geogit.api.plumbing.DiffBounds;
import org.geogit.api.plumbing.DiffCount;
import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.api.plumbing.diff.DiffObjectCount;
import org.geogit.api.plumbing.diff.DiffSummary;
import org.geogit.api.porcelain.DiffOp;
import org.geogit.cli.AbstractCommand;
import org.geogit.cli.AnsiDecorator;
import org.geogit.cli.CLICommand;
import org.geogit.cli.GeogitCLI;
import org.geogit.cli.InvalidParameterException;
import org.geogit.cli.annotation.ReadOnly;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * Shows changes between commits, commits and working tree, etc.
 * <p>
 * Usage:
 * <ul>
 * <li> {@code geogit diff [-- <path>...]}: compare working tree and index
 * <li> {@code geogit diff <commit> [-- <path>...]}: compare the working tree with the given commit
 * <li> {@code geogit diff --cached [-- <path>...]}: compare the index with the HEAD commit
 * <li> {@code geogit diff --cached <commit> [-- <path>...]}: compare the index with the given commit
 * <li> {@code geogit diff <commit1> <commit2> [-- <path>...]}: compare {@code commit1} with
 * {@code commit2}, where {@code commit1} is the eldest or left side of the diff.
 * </ul>
 * 
 * @see DiffOp
 */
@ReadOnly
@Parameters(commandNames = "diff", commandDescription = "Show changes between commits, commit and working tree, etc")
public class Diff extends AbstractCommand implements CLICommand {

    @Parameter(description = "[<commit> [<commit>]] [-- <path>...]", arity = 2)
    private List<String> refSpec = Lists.newArrayList();

    @Parameter(names = "--", hidden = true, variableArity = true)
    private List<String> paths = Lists.newArrayList();

    @Parameter(names = "--cached", description = "compares the specified tree (commit, branch, etc) and the staging area")
    private boolean cached;

    @Parameter(names = "--summary", description = "List only summary of changes")
    private boolean summary;

    @Parameter(names = "--nogeom", description = "Do not show detailed coordinate changes in geometries")
    private boolean nogeom;

    @Parameter(names = "--bounds", description = "Show only the bounds of the difference between the two trees")
    private boolean bounds;

    @Parameter(names = "--crs", description = "Coordinate reference system for --bounds (defaults to EPSG:4326 with lon/lat axis order)")
    private String boundsCrs;

    @Parameter(names = "--count", description = "Only count the number of changes between the two trees")
    private boolean count;

    /**
     * Executes the diff command with the specified options.
     */
    @Override
    protected void runInternal(GeogitCLI cli) throws IOException {
        checkParameter(refSpec.size() <= 2, "Commit list is too long :%s", refSpec);
        checkParameter(!(nogeom && summary), "Only one printing mode allowed");
        checkParameter(!(bounds && count), "Only one of --bounds or --count is allowed");
        checkParameter(!(cached && refSpec.size() > 1),
                "--cached allows zero or one ref specs to compare the index with.");

        GeoGIT geogit = cli.getGeogit();

        String oldVersion = resolveOldVersion();
        String newVersion = resolveNewVersion();

        List<String> paths = removeEmptyPaths();
        if (bounds) {
            DiffBounds diff = geogit.command(DiffBounds.class).setOldVersion(oldVersion)
                    .setNewVersion(newVersion).setCompareIndex(cached);
            diff.setPathFilters(paths);
            CoordinateReferenceSystem crs = parseCrs();
            if (crs != null) {
                diff.setCRS(crs);
            }
            DiffSummary<BoundingBox, BoundingBox> diffBounds = diff.call();
            BoundsDiffPrinter.print(geogit, cli.getConsole(), diffBounds);
            return;
        }
        if (count) {
            if (oldVersion == null) {
                oldVersion = Ref.HEAD;
            }
            if (newVersion == null) {
                newVersion = cached ? Ref.STAGE_HEAD : Ref.WORK_HEAD;
            }
            DiffCount cdiff = geogit.command(DiffCount.class).setOldVersion(oldVersion)
                    .setNewVersion(newVersion);
            cdiff.setFilter(paths);
            DiffObjectCount count = cdiff.call();
            ConsoleReader console = cli.getConsole();
            console.println(String.format("Trees changed: %d, features changed: %,d",
                    count.treeCount(), count.featureCount()));
            console.flush();
            return;
        }

        DiffOp diff = geogit.command(DiffOp.class);
        diff.setOldVersion(oldVersion).setNewVersion(newVersion).setCompareIndex(cached);

        Iterator<DiffEntry> entries;
        if (paths.isEmpty()) {
            entries = diff.setProgressListener(cli.getProgressListener()).call();
        } else {
            entries = Iterators.emptyIterator();
            for (String path : paths) {
                Iterator<DiffEntry> moreEntries = diff.setFilter(path)
                        .setProgressListener(cli.getProgressListener()).call();
                entries = Iterators.concat(entries, moreEntries);
            }
        }

        if (!entries.hasNext()) {
            cli.getConsole().println("No differences found");
            return;
        }

        DiffPrinter printer;
        if (summary) {
            printer = new SummaryDiffPrinter();
        } else {
            printer = new FullDiffPrinter(nogeom, false);
        }

        DiffEntry entry;
        while (entries.hasNext()) {
            entry = entries.next();
            printer.print(geogit, cli.getConsole(), entry);
        }
    }

    private List<String> removeEmptyPaths() {
        List<String> paths = Lists.newLinkedList(this.paths);
        for (Iterator<String> it = paths.iterator(); it.hasNext();) {
            if (Strings.isNullOrEmpty(it.next())) {
                it.remove();
            }
        }
        return paths;
    }

    private CoordinateReferenceSystem parseCrs() {
        if (boundsCrs == null) {
            return null;
        }
        try {
            return CRS.decode(boundsCrs, true);
        } catch (Exception e) {
            throw new InvalidParameterException(String.format("Unrecognized CRS: '%s'", boundsCrs));
        }
    }

    @Nullable
    private String resolveOldVersion() {
        return refSpec.size() > 0 ? refSpec.get(0) : null;
    }

    @Nullable
    private String resolveNewVersion() {
        return refSpec.size() > 1 ? refSpec.get(1) : null;
    }

    private static final class BoundsDiffPrinter {

        public static void print(GeoGIT geogit, ConsoleReader console,
                DiffSummary<BoundingBox, BoundingBox> diffBounds) throws IOException {

            BoundingBox left = diffBounds.getLeft();
            BoundingBox right = diffBounds.getRight();
            Optional<BoundingBox> mergedResult = diffBounds.getMergedResult();
            BoundingBox both = new ReferencedEnvelope();
            if (mergedResult.isPresent()) {
                both = mergedResult.get();
            }

            Ansi ansi = AnsiDecorator.newAnsi(console.getTerminal().isAnsiSupported());

            ansi.a("left:  ").a(bounds(left)).newline();
            ansi.a("right: ").a(bounds(right)).newline();
            ansi.a("both:  ").a(bounds(both)).newline();
            ansi.a("CRS:   ").a(CRS.toSRS(left.getCoordinateReferenceSystem())).newline();

            console.print(ansi.toString());
        }

        private static CharSequence bounds(BoundingBox b) {
            if (b.isEmpty()) {
                return "<empty>";
            }
            return String.format("%f,%f,%f,%f", b.getMinX(), b.getMinY(), b.getMaxX(), b.getMaxY());
        }

    }
}
