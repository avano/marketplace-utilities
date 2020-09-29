package io.syndesis.qe.marketplace.manifests;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.openshift.client.OpenShiftClient;
import io.syndesis.qe.marketplace.openshift.OpenShiftService;
import io.syndesis.qe.marketplace.quay.QuayService;
import io.syndesis.qe.marketplace.quay.QuayUser;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static io.syndesis.qe.marketplace.util.HelperFunctions.readResource;
import static io.syndesis.qe.marketplace.util.HelperFunctions.waitFor;

public class Index {
    private final String name;
    @Getter
    private String ocpName;
    private List<Bundle> bundles;
    private Opm opm;
    private boolean isPushed;
    private static final String BUILD_TOOL = "docker";
    static final String MARKETPLACE_NAMESPACE = "openshift-marketplace";
    private static QuayService quaySvc;

    Index(String name) {
        this.name = name;
        bundles = new ArrayList<>();
        isPushed = false;
    }

    void setOpm(Opm opm) {
        this.opm = opm;
    }

    public Bundle addBundle(String bundleName) {
        int ret = opm.runCmd("index", "add", "--bundles=" + bundleName, "--tag=" + this.name, "--build-tool=" + BUILD_TOOL);
        isPushed = false;
        Bundle bundle = new Bundle(bundleName, this);
        bundles.add(bundle);
        return bundle;
    }

    public void addBundles(String... names) {
        for (String name : names) {
            addBundle(name);
        }
    }

    @SneakyThrows
    private void cmdWrapper(String... command) {
        Process process = new ProcessBuilder(command).start();
        int ret = process.waitFor();
        if (ret != 0) {
            String out = IOUtils.toString(process.getErrorStream(), Charset.defaultCharset());
            System.err.println(out);
        }
    }

    @SneakyThrows
    public void push(QuayUser user) {
        if (quaySvc == null) {
            quaySvc = new QuayService(user, null, null);
        }
        cmdWrapper(BUILD_TOOL, "login", "-u=" + user.getUserName(), "-p=" + user.getPassword(), "quay.io");
        cmdWrapper(BUILD_TOOL, "push", name);
        String[] parts = name.split("/");
        String name = parts[parts.length - 1].replaceAll(":.*", "");
        String token = quaySvc.loginToQuayAndGetToken();
        quaySvc.changeProjectVisibilityToPublic(name);
        isPushed = true;
    }

    private static CustomResourceDefinitionContext catalogSourceIndex() {
        return new CustomResourceDefinitionContext.Builder()
                .withGroup("operators.coreos.com")
                .withVersion("v1alpha1")
                .withName("CatalogSource")
                .withPlural("catalogsources")
                .withScope("Namespaced")
                .build();
    }

    public void addIndexToCluster(OpenShiftService service, String catalogName) throws IOException, TimeoutException, InterruptedException {
        if (!isPushed) {
            throw new IllegalStateException("You forgot to push the index image to quay");
        }
        OpenShiftClient ocp = service.getClient();

        String catalogSource = null;
        try {
            catalogSource = readResource("openshift/create-operatorsourceindex.yaml");
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.ocpName = catalogName;
        catalogSource = catalogSource.replaceAll("IMAGE", name)
                .replaceAll("DISPLAY_NAME", "Test catalog")
                .replaceAll("NAME", catalogName);
        ocp.customResource(catalogSourceIndex()).createOrReplace(MARKETPLACE_NAMESPACE, catalogSource);
        Predicate<Pod> podFound = pod ->
                pod.getMetadata().getName().startsWith(catalogName)
                        && "Running".equalsIgnoreCase(pod.getStatus().getPhase());
        waitFor(() -> ocp.pods().inNamespace("openshift-marketplace").list()
                .getItems().stream().anyMatch(podFound), 5, 60 * 1000);
    }

    public void removeIndexFromCluster(OpenShiftService service) {
        service.getClient().customResource(catalogSourceIndex()).delete(MARKETPLACE_NAMESPACE, ocpName);
    }
}
