package org.geogit.cli.test;

import static org.geogit.cli.test.functional.GlobalState.currentDirectory;
import static org.geogit.cli.test.functional.GlobalState.geogit;
import static org.geogit.cli.test.functional.GlobalState.homeDirectory;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.geogit.api.InjectorBuilder;
import org.geogit.api.porcelain.AddOp;
import org.geogit.api.porcelain.CommitOp;
import org.geogit.cli.GeogitPy4JEntryPoint;
import org.geogit.cli.test.functional.AbstractGeogitFunctionalTest;
import org.geogit.cli.test.functional.CLITestInjectorBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import py4j.GatewayServer;

public class GeogitPy4JEntryPointTest extends AbstractGeogitFunctionalTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUpDirectories() throws IOException {
        tempFolder.create();
        homeDirectory = tempFolder.newFolder("fakeHomeDir").getCanonicalFile();
        currentDirectory = tempFolder.newFolder("testrepo").getCanonicalFile();
    }

    @Override
    protected InjectorBuilder getInjectorBuilder() {
        return new CLITestInjectorBuilder(currentDirectory, homeDirectory);
    }

    @Test
    public void testPy4JentryPoint() throws Exception {
        setupGeogit();
        setupFeatures();
        String repoFolder = currentDirectory.getAbsolutePath();
        GeogitPy4JEntryPoint py4j = new GeogitPy4JEntryPoint();
        GatewayServer gatewayServer = new GatewayServer(py4j);
        gatewayServer.start();
        py4j.runCommand(repoFolder, new String[] { "init" });
        py4j.runCommand(repoFolder, "config user.name name".split(" "));
        py4j.runCommand(repoFolder, "config user.email email@email.com".split(" "));
        insert(points1);
        insert(points2);
        insert(points3);
        geogit.command(AddOp.class).call();
        geogit.command(CommitOp.class).setMessage("message").call();
        py4j.runCommand(repoFolder, new String[] { "log" });
        String output = py4j.lastOutput();
        assertTrue(output.contains("message"));
        assertTrue(output.contains("name"));
        assertTrue(output.contains("email@email.com"));
        insert(points1_modified);
        py4j.runCommand(repoFolder, new String[] { "add" });
        py4j.runCommand(repoFolder, new String[] { "commit", "-m", "a commit message" });
        py4j.runCommand(repoFolder, new String[] { "log" });
        output = py4j.lastOutput();
        System.out.println(output);
        assertTrue(output.contains("a commit message"));

        gatewayServer.shutdown();
    }
}
