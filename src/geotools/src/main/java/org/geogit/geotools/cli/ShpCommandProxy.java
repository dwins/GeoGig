/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.geotools.cli;

import org.geogit.cli.CLICommandExtension;
import org.geogit.geotools.cli.porcelain.ShpExport;
import org.geogit.geotools.cli.porcelain.ShpExportDiff;
import org.geogit.geotools.cli.porcelain.ShpImport;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

/**
 * {@link CLICommandExtension} that provides a {@link JCommander} for shapefile specific commands.
 * <p>
 * Usage:
 * <ul>
 * <li> {@code geogit shp <command> <args>...}
 * </ul>
 * 
 * @see ShpImport
 */
@Parameters(commandNames = "shp", commandDescription = "GeoGit/Shapefile integration utilities")
public class ShpCommandProxy implements CLICommandExtension {

    /**
     * @return the JCommander parser for this extension
     * @see JCommander
     */
    @Override
    public JCommander getCommandParser() {
        JCommander commander = new JCommander();
        commander.setProgramName("geogit shp");
        commander.addCommand("import", new ShpImport());
        commander.addCommand("export", new ShpExport());
        commander.addCommand("export-diff", new ShpExportDiff());
        return commander;
    }
}
