/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */

package org.geogit.cli.plumbing;

import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.List;

import jline.console.ConsoleReader;

import org.geogit.api.ObjectId;
import org.geogit.api.RevObject;
import org.geogit.api.plumbing.WalkGraphOp;
import org.geogit.cli.AbstractCommand;
import org.geogit.cli.CLICommand;
import org.geogit.cli.GeogitCLI;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 *
 */
@Parameters(commandNames = "walk-graph", commandDescription = "Visit objects in history graph in post order (referenced objects before referring objects)")
public class WalkGraph extends AbstractCommand implements CLICommand {

    @Parameter(description = "<[refspec]:[path]>", arity = 1)
    private List<String> refList = Lists.newArrayList();

    @Parameter(names = { "-v", "--verbose" }, description = "Verbose output, include metadata, object id, and object type among object path.")
    private boolean verbose;

    @Override
    public void runInternal(GeogitCLI cli) throws Exception {
        checkState(cli.getGeogit() != null, "Not a geogit repository: " + cli.getPlatform().pwd());
        String ref;
        if (refList.isEmpty()) {
            ref = null;
        } else {
            ref = refList.get(0);
        }
        Iterator<RevObject> iter = 
            cli.getGeogit() //
               .command(WalkGraphOp.class)
               .setReference(ref) //
               // .setStrategy(lsStrategy) //
               .call();

        final ConsoleReader console = cli.getConsole();
        if (!iter.hasNext()) {
            if (ref == null) {
                console.println("The working tree is empty");
            } else {
                console.println("The specified path is empty");
            }
            return;
        }

        Function<RevObject, CharSequence> printFunctor = new Function<RevObject, CharSequence>() {
            @Override
            public CharSequence apply(RevObject input) {
                if (verbose) {
                    return String.format("%s: %s %s", input.getId(), input.getType(), input);
                } else {
                    return String.format("%s: %s", input.getId(), input.getType());
                }
            }
        };

        Iterator<CharSequence> lines = Iterators.transform(iter, printFunctor);

        while (lines.hasNext()) {
            console.println(lines.next());
        }
        console.flush();
    }
}
