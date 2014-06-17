/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.geogit.api.DefaultProgressListener;
import org.geogit.api.RevCommit;
import org.geogit.api.RevTree;
import org.geogit.api.plumbing.diff.DiffSummary;
import org.geogit.api.porcelain.AddOp;
import org.geogit.api.porcelain.CommitOp;
import org.geogit.repository.WorkingTree;
import org.geogit.test.integration.RepositoryTestCase;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.io.ParseException;

public class DiffBoundsTest extends RepositoryTestCase {

    private static final CoordinateReferenceSystem DEFAULT_CRS;
    static {
        try {
            DEFAULT_CRS = CRS.decode("EPSG:4326", true);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private Feature l1Modified;

    private Feature l2Modified;

    private RevCommit points1_modified_commit;

    @Override
    protected void setUpInternal() throws Exception {
        // create one commit per feature
        ArrayList<RevCommit> commits = Lists.newArrayList(populate(true, points1, points3,
                points1_modified));
        this.points1_modified_commit = commits.get(2);

        Feature p1ModifiedAgain = feature(pointsType, idP1, "StringProp1_1a", new Integer(1001),
                "POINT(10 20)");// used to be POINT(1 2)
        insertAndAdd(p1ModifiedAgain);
        commits.add(geogit.command(CommitOp.class).call());

        points1B_modified = feature(pointsType, idP1, "StringProp1B_1a", new Integer(2000),
                "POINT(10 220)");
        insertAndAdd(points1B_modified);
        commits.add(geogit.command(CommitOp.class).call());

        l1Modified = feature(linesType, idL1, "StringProp2_1", new Integer(1000),
                "LINESTRING (1 1, -2 -2)");// used to be LINESTRING (1 1, 2 2)

        l2Modified = feature(linesType, idL2, "StringProp2_2", new Integer(2000),
                "LINESTRING (3 3, 4 4)");// used to be LINESTRING (3 3, 4 4)
    }

    @Test
    public void testDiffBetweenDifferentTrees() {
        String oldRefSpec = "HEAD~3";
        String newRefSpec = "HEAD";

        DiffSummary<BoundingBox, BoundingBox> diffBounds = geogit.command(DiffBounds.class)
                .setOldVersion(oldRefSpec).setNewVersion(newRefSpec)
                .setCRS(pointsType.getCoordinateReferenceSystem()).call();

        BoundingBox bounds = diffBounds.getMergedResult().get();
        assertEquals(1.0, bounds.getMinX(), 0.0);
        assertEquals(1.0, bounds.getMinY(), 0.0);
        assertEquals(10.0, bounds.getMaxX(), 0.0);
        assertEquals(220.0, bounds.getMaxY(), 0.0);
    }

    @Test
    public void testDiffBetweenIdenticalTrees() {
        String oldRefSpec = "HEAD";
        String newRefSpec = "HEAD";

        DiffSummary<BoundingBox, BoundingBox> diffBounds = geogit.command(DiffBounds.class)
                .setOldVersion(oldRefSpec).setNewVersion(newRefSpec).call();
        assertTrue(diffBounds.getLeft().isEmpty());
        assertTrue(diffBounds.getRight().isEmpty());
        assertTrue(diffBounds.getMergedResult().get().isEmpty());
    }

    @Test
    public void testPathFiltering() throws Exception {
        insertAndAdd(l1Modified);
        geogit.command(CommitOp.class).call();
        insert(l2Modified);

        testPathFiltering("HEAD~3", "HEAD", l1Modified.getBounds(), linesName);
        testPathFiltering("HEAD", "WORK_HEAD", l2Modified.getBounds(), linesName);
        testPathFiltering("HEAD~3", "HEAD~2", null, linesName);
        testPathFiltering("HEAD~3", "HEAD~2", null, linesName);

        String head = points1_modified_commit.getId().toString();

        BoundingBox expected = points1.getBounds();
        expected.include(points1_modified.getBounds());

        testPathFiltering(head + "^", head, expected, pointsName);
        testPathFiltering(head + "^", head, null, linesName);
        testPathFiltering("HEAD^", "HEAD", null, pointsName);
    }

    private void testPathFiltering(String oldVersion, String newVersion,
            @Nullable BoundingBox expected, @Nullable String... pathFilters) {

        List<String> filter = ImmutableList.<String> copyOf(pathFilters);

        CoordinateReferenceSystem crs = DEFAULT_CRS;
        if (expected != null) {
            crs = expected.getCoordinateReferenceSystem();
        }
        DiffSummary<BoundingBox, BoundingBox> result = geogit.command(DiffBounds.class)//
                .setOldVersion(oldVersion)//
                .setNewVersion(newVersion)//
                .setPathFilters(filter)//
                .setCRS(crs)//
                .call();

        BoundingBox actual = result.getMergedResult().get();
        if (null == expected) {
            assertTrue(actual.isEmpty());
        } else {
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testDefaultCrs() {
        DiffSummary<BoundingBox, BoundingBox> diffBounds = geogit.command(DiffBounds.class)
                .setOldVersion("HEAD^").setNewVersion("HEAD").call();

        assertEquals(DEFAULT_CRS, diffBounds.getLeft().getCoordinateReferenceSystem());
        assertEquals(DEFAULT_CRS, diffBounds.getRight().getCoordinateReferenceSystem());
        assertEquals(DEFAULT_CRS, diffBounds.getMergedResult().get().getCoordinateReferenceSystem());
    }

    @Test
    public void testReprojectToTargetCRS() throws Exception {
        DiffBounds cmd = geogit.command(DiffBounds.class).setOldVersion("HEAD^")
                .setNewVersion("HEAD");

        DiffSummary<BoundingBox, BoundingBox> defaultCrs = cmd.call();

        CoordinateReferenceSystem target = CRS.decode("EPSG:26986");
        cmd.setCRS(target);
        DiffSummary<BoundingBox, BoundingBox> reprojected = cmd.call();

        assertEquals(target, reprojected.getLeft().getCoordinateReferenceSystem());
        assertEquals(target, reprojected.getRight().getCoordinateReferenceSystem());
        assertEquals(target, reprojected.getMergedResult().get().getCoordinateReferenceSystem());

        assertFalse(defaultCrs.getLeft().isEmpty());
        assertFalse(defaultCrs.getRight().isEmpty());
        assertFalse(defaultCrs.getMergedResult().get().isEmpty());

        ReferencedEnvelope e = new ReferencedEnvelope(defaultCrs.getLeft());
        ReferencedEnvelope expected = e.transform(target, true);
        assertEquals(expected, reprojected.getLeft());
    }

    @Test
    public void testReprojectToTargetBucketTree() throws Exception {
        final int leftCount = RevTree.NORMALIZED_SIZE_LIMIT * 2;
        final int rightCount = RevTree.NORMALIZED_SIZE_LIMIT * 3;

        WorkingTree workingTree = geogit.getRepository().workingTree();
        final String typeName = "newpoints";

        final DefaultProgressListener listener = new DefaultProgressListener();
        workingTree.insert(typeName, new TestFeatureIterator(typeName, leftCount), listener, null,
                null);
        geogit.command(AddOp.class).call();

        workingTree.insert(typeName, new TestFeatureIterator(typeName, rightCount), listener, null,
                null);

        {// sanity check
            long diffFeatures = geogit.command(DiffCount.class).setOldVersion("STAGE_HEAD")
                    .setNewVersion("WORK_HEAD").call().featureCount();
            assertEquals(rightCount - leftCount, diffFeatures);
        }

        DiffBounds cmd = geogit.command(DiffBounds.class).setOldVersion("STAGE_HEAD")
                .setNewVersion("WORK_HEAD");

        final CoordinateReferenceSystem nativeCrs = CRS.decode("EPSG:3857");
        final DiffSummary<BoundingBox, BoundingBox> diffInNativeCrs = cmd.setCRS(nativeCrs).call();

        CoordinateReferenceSystem targetcrs = CRS.decode("EPSG:4326", true);
        cmd.setCRS(targetcrs);
        DiffSummary<BoundingBox, BoundingBox> reprojected = cmd.call();

        assertEquals(targetcrs, reprojected.getLeft().getCoordinateReferenceSystem());
        assertEquals(targetcrs, reprojected.getRight().getCoordinateReferenceSystem());
        assertEquals(targetcrs, reprojected.getMergedResult().get().getCoordinateReferenceSystem());

        ReferencedEnvelope e = new ReferencedEnvelope(diffInNativeCrs.getRight());
        ReferencedEnvelope expected = e.transform(targetcrs, true);
        BoundingBox actual = reprojected.getRight();
        assertEquals(expected, actual);
    }

    private final class TestFeatureIterator extends AbstractIterator<Feature> {
        final int fcount;

        int c;

        private SimpleFeatureType featureType;

        TestFeatureIterator(String typeName, int featureCount) {
            try {
                String typeSpec = "pp:Point:srid=3857";
                this.featureType = DataUtilities.createType(typeName, typeSpec);
            } catch (SchemaException e) {
                throw Throwables.propagate(e);
            }
            this.fcount = featureCount;
        }

        @Override
        protected Feature computeNext() {
            c++;
            if (c == fcount) {
                return endOfData();
            }
            try {
                String geomWkt = String.format("POINT(%d %d)", c, c);
                return feature(featureType, String.valueOf(c), geomWkt);
            } catch (ParseException e) {
                throw Throwables.propagate(e);
            }
        }
    }

}
