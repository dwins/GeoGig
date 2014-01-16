/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.cli;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.console.ConsoleReader;

import org.geogit.api.DefaultPlatform;

import py4j.GatewayServer;

import com.google.common.base.Throwables;

/**
 * Provides an entry point using the py4j library, to expose GeoGit functionality to python
 * applications
 */
public class GeogitPy4JEntryPoint {

    ConsoleReader consoleReader;

    /**
     * A class to parse and store console output of GeoGit commands
     */
    public class ToStringOutputStream extends OutputStream {

        StringBuffer sb = new StringBuffer();

        StringBuffer progressSubstring = new StringBuffer();

        private GeoGitPy4JProgressListener listener;

        public ToStringOutputStream() {
            this.listener = new SilentProgressListener();
        }

        @Override
        public void write(int b) throws IOException {
            char c = (char) b;
            String s = String.valueOf(c);
            if (c == '\n' || c == '%') {
                try {
                    int progress = Integer.parseInt(progressSubstring.toString().replace("%", "")
                            .trim());
                    listener.setProgress(progress);
                } catch (NumberFormatException e) {
                    sb.append(s);
                }
                progressSubstring = new StringBuffer();
            } else {
                progressSubstring.append(s);
                sb.append(s);
            }
        }

        public void setProgressListener(GeoGitPy4JProgressListener listener) {
            this.listener = listener;
        }

        public void clear() {
            sb = new StringBuffer();
        }

        public String asString() {
            return sb.toString();
        }
    }

    private PrintStream stream;

    private ToStringOutputStream os;

    public GeogitPy4JEntryPoint() {

        os = new ToStringOutputStream();
        stream = new PrintStream(os);
        try {
            consoleReader = new ConsoleReader(System.in, stream);
            consoleReader.getTerminal().setEchoEnabled(true);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private String[] tokenizeString(String s) {
        List<String> list = new ArrayList<String>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(s);
        while (m.find())
            list.add(m.group(1).replace("\"", ""));
        return list.toArray(new String[0]);
    }

    /**
     * Runs a command on a given repository
     * 
     * @param folder the repository folder
     * @param command the command to run
     * @return
     */
    public int runCommand(String folder, String command) {
        System.gc();
        GeogitCLI cli = new GeogitCLI(consoleReader);
        cli.tryConfigureLogging();
        DefaultPlatform platform = new DefaultPlatform();
        platform.setWorkingDir(new File(folder));
        cli.setPlatform(platform);
        String[] tokens = tokenizeString(command);
        os.clear();
        System.out.print("Running command: " + command);
        int ret = cli.processCommand(tokens);
        cli.close();
        if (ret == 0) {
            System.out.println(" [OK]");
        } else {
            System.out.println(" [Error]");
        }

        return ret;
    }

    /**
     * Returns the output of the last command
     * 
     * @return the output of the last commmand
     * @throws IOException
     */
    public String lastOutput() throws IOException {
        stream.flush();
        os.flush();
        return os.asString();
    }

    public boolean isGeoGitServer() {
        return true;
    }

    /**
     * Shutdowns the server
     */
    public void shutdown() {
        System.out.println("Shutting down GeoGit server.");
        System.exit(0);
    }

    /**
     * Sets the progress listener that will receive progress updates
     * 
     * @param listener
     */
    public void setProgressListener(GeoGitPy4JProgressListener listener) {
        os.setProgressListener(listener);
    }

    /**
     * Removes the progress listener, if set
     */
    public void removeProgressListener() {
        os.setProgressListener(new SilentProgressListener());
    }

    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(new GeogitPy4JEntryPoint());
        gatewayServer.start();
        System.out.println("GeoGit server correctly started and waiting for conections at port "
                + Integer.toString(gatewayServer.getListeningPort()));
    }
}

class SilentProgressListener implements GeoGitPy4JProgressListener {

    @Override
    public void setProgress(int i) {

    }

}