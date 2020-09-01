/*
 * Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JtregJextractSources {

    public static int main(String[] args) throws IOException {
        Path sourcePath = Path.of("sources");
        JtregJextract jj =  new JtregJextract(null, sourcePath);
        String[] newArgs = new String[args.length + 1];
        newArgs[0] = "--source";
        System.arraycopy(args, 0, newArgs, 1, args.length);
        jj.jextract(newArgs);

        Path outputDir = Paths.get(System.getProperty("test.classes", "."));

        List<String> files = Files.find(sourcePath.toAbsolutePath(), 999, (path, ignored) -> path.toString().endsWith(".java"))
                .map(p -> p.toAbsolutePath().toString())
                .collect(Collectors.toList());

        List<String> commands = new ArrayList<>();
        commands.add(Paths.get(System.getProperty("test.jdk"), "bin", "javac").toString());
        commands.add("--add-modules");
        commands.add("jdk.incubator.foreign");
        commands.add("-d");
        commands.add(outputDir.toAbsolutePath().toString());
        commands.addAll(files);

        try {
            Process proc = new ProcessBuilder(commands).inheritIO().start();
            int result = proc.waitFor();
            if (result != 0) {
                throw new RuntimeException("javac returns non-zero value: " + result);
            }
            return result;
        } catch (IOException ioExp) {
            throw new UncheckedIOException(ioExp);
        } catch (InterruptedException intExp) {
            throw new RuntimeException(intExp);
        }
    }
}
