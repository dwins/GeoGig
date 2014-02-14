/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.cli.porcelain;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.geogit.api.GeoGIT;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.geogit.api.porcelain.InitOp;
import org.geogit.cli.AbstractCommand;
import org.geogit.cli.CLICommand;
import org.geogit.cli.CommandFailedException;
import org.geogit.cli.GeogitCLI;
import org.geogit.cli.RequiresRepository;
import org.geogit.di.PluginDefaults;
import org.geogit.di.VersionedFormat;
import org.geogit.repository.Repository;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

/**
 * This command creates an empty geogit repository - basically a .geogit directory with
 * subdirectories for the object, refs, index, and config databases. An initial HEAD that references
 * the HEAD of the master branch is also created.
 * <p>
 * CLI proxy for {@link InitOp}
 * <p>
 * Usage:
 * <ul>
 * <li> {@code geogit init [<directory>]}
 * </ul>
 * 
 * @see InitOp
 */
@RequiresRepository(false)
@Parameters(commandNames = "init", commandDescription = "Create an empty geogit repository or reinitialize an existing one")
public class Init extends AbstractCommand implements CLICommand {

    @Parameter(description = "Repository location (directory).", required = false, arity = 1)
    private List<String> location;

    @Parameter(names = { "--config" }, description = "Extra configuration options to set while preparing repository. Separate names from values with an equals sign and delimit configuration options with a colon. Example: storage.objects=bdbje:bdbje.version=0.1")
    private String config;

    private void addDefaults(PluginDefaults defaults, ImmutableList.Builder<String> builder) {
        Optional<VersionedFormat> refs = defaults.getRefs();
        Optional<VersionedFormat> objects = defaults.getObjects();
        Optional<VersionedFormat> staging = defaults.getStaging();
        Optional<VersionedFormat> graph = defaults.getGraph();
        if (refs.isPresent()) {
            builder.add("storage.refs");
            builder.add(refs.get().getFormat());
            builder.add(refs.get().getFormat() + ".version");
            builder.add(refs.get().getVersion());
        }
        if (objects.isPresent()) {
            builder.add("storage.objects");
            builder.add(objects.get().getFormat());
            builder.add(objects.get().getFormat() + ".version");
            builder.add(objects.get().getVersion());
        }
        if (staging.isPresent()) {
            builder.add("storage.staging");
            builder.add(staging.get().getFormat());
            builder.add(staging.get().getFormat() + ".version");
            builder.add(staging.get().getVersion());
        }
        if (graph.isPresent()) {
            builder.add("storage.graph");
            builder.add(graph.get().getFormat());
            builder.add(graph.get().getFormat() + ".version");
            builder.add(graph.get().getVersion());
        }
    }

    /**
     * Executes the init command.
     */
    @Override
    public void runInternal(GeogitCLI cli) throws IOException {
        final File repoDir;
        {
            File currDir = cli.getPlatform().pwd();
            if (location != null && location.size() == 1) {
                String target = location.get(0);
                File f = new File(target);
                if (!f.isAbsolute()) {
                    f = new File(currDir, target).getCanonicalFile();
                }
                repoDir = f;
                if (!repoDir.exists() && !repoDir.mkdirs()) {
                    throw new CommandFailedException("Can't create directory "
                            + repoDir.getAbsolutePath());
                }
            } else {
                repoDir = currDir;
            }
        }

        GeoGIT geogit = null;
        if (cli.getGeogit() == null) {
            geogit = new GeoGIT(cli.getGeogitInjector(), repoDir);
        } else {
            geogit = cli.getGeogit();
        }

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        addDefaults(cli.getGeogitInjector().getInstance(PluginDefaults.class), builder);
        builder.addAll(splitConfig(config));
        List<String> effectiveConfiguration = builder.build();

        Repository repository = geogit.command(InitOp.class).setConfig(effectiveConfiguration)
                .call();
        final boolean repoExisted = repository == null;
        geogit.setRepository(repository);
        cli.setGeogit(geogit);

        final URL envHome = geogit.command(ResolveGeogitDir.class).call();

        File repoDirectory;
        try {
            repoDirectory = new File(envHome.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Environment home can't be resolved to a directory", e);
        }
        String message;
        if (repoExisted) {
            message = "Reinitialized existing Geogit repository in "
                    + repoDirectory.getAbsolutePath();
        } else {
            message = "Initialized empty Geogit repository in " + repoDirectory.getAbsolutePath();
        }
        cli.getConsole().println(message);
    }

    public static List<String> splitConfig(String config) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (config != null) {
            String[] options = config.split(",");
            for (String option : options) {
                String[] kv = option.split("=", 2);
                if (kv.length < 2)
                    continue;
                builder.add(kv[0], kv[1]);
            }
        }
        return builder.build();
    }
}
