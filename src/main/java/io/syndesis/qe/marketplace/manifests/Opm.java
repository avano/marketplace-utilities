package io.syndesis.qe.marketplace.manifests;

import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableObject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class Opm {

    private File binary;
    private static final String OPM_IMAGE = "registry.redhat.io/openshift4/ose-operator-registry:";
    private static final String version = "v4.5";

    public Opm() {
        try {
            Process which = new ProcessBuilder("which", "opm").start();
            int result = which.waitFor();
            String path = IOUtils.toString(which.getInputStream(), Charset.defaultCharset()).trim();
            if (result != 0) {
                fetchOpm(version);
            } else {
                binary = new File(path);
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private Opm(String version) {
        fetchOpm(version);
    }

    private void fetchOpm(String version) {
        if (binary.exists())
            return;
        try {
            binary = File.createTempFile("opm", version);
            new ProcessBuilder("docker", "run", "--rm", "--entrypoint=bash", OPM_IMAGE + version, "-c 'cat /usr/bin/opm'")
                    .redirectOutput(binary)
                    .start()
                    .waitFor();
            binary.setExecutable(true);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @SneakyThrows
    int runCmd(String... args) {
        String[] command = new String[args.length + 1];
        System.arraycopy(args, 0, command, 1, args.length);
        command[0] = binary.getAbsolutePath();
        Process process = new ProcessBuilder(command).start();
        int ret = process.waitFor();
        if (ret != 0) {
            String out = IOUtils.toString(process.getErrorStream(), Charset.defaultCharset());
            System.err.println(out);
        }
        return ret;
    }


    @SneakyThrows
    int runCmd(MutableObject<String> refOutput, String... args) {
        Process p = new ProcessBuilder(binary.getAbsolutePath()).command(args).start();
        refOutput.setValue(IOUtils.toString(p.getInputStream(), Charset.defaultCharset()));
        return p.waitFor();
    }

    public Index createIndex(String name) {
        Index index = new Index(name);
        index.setOpm(this);
        return index;
    }
}
