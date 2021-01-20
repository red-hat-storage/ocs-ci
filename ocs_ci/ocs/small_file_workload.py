"""
Test to exercise Small File Workload

Note:
This test is using the ripsaw and the elastic search, so it start process with
port forwarding on port 9200 from the host that run the test (localhost) to
the elastic-search within the open-shift cluster, so, if you host is listen to
port 9200, this test can not be running in your host.

"""

# Builtin modules
import logging

# Local modules
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.version import get_environment_info
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


def smallfile_workload(
    ripsaw, es, scale_leaks, file_size, files, threads, samples, interface
):
    """
    Run SmallFile Workload
    SmallFile workload uses https://github.com/distributed-system-analysis/smallfile

    smallfile is a python-based distributed POSIX workload generator which can be
    used to quickly measure performance and scaling for a variety of metadata-intensive
    workloads

    Args:
        ripsaw -- Ripsaw fixture to setup/teardown ripsaw
        es -- Elastic search fixture
        scale_leaks -- scale leak checking fixture
        file_siles -- size of file to be created
        files -- number of files to be created
        threads -- number of threads to run
        samples -- samples taken if running performance tests
        interface -- CephFileSystem or CephBlockPool

    Returns:
        backup_es (str) -- backup elastic search location
        environment (dict) -- environment (user is changed at times)
        sf_data (dict) -- small file data extracted from yaml and modified by this routine

    """

    # Loading the main template yaml file for the benchmark
    sf_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)

    # Saving the Original elastic-search IP and PORT - if defined in yaml
    if "elasticsearch" in sf_data["spec"]:
        sf_data["spec"]["elasticsearch"][
            "url"
        ] = f"http://{sf_data['spec']['elasticsearch']['server']}:{sf_data['spec']['elasticsearch']['port']}"
        backup_es = sf_data["spec"]["elasticsearch"]
    else:
        log.warning("Elastic Search information does not exists in YAML file")
        sf_data["spec"]["elasticsearch"] = {}

    # Use the internal define elastic-search server in the test - if exist
    if es:
        sf_data["spec"]["elasticsearch"] = {
            "url": f"http://{es.get_ip()}:{es.get_port()}",
            "server": es.get_ip(),
            "port": es.get_port(),
        }

    log.info("Apply Operator CRD")
    ripsaw.apply_crd("resources/crds/ripsaw_v1alpha1_ripsaw_crd.yaml")
    if interface == constants.CEPHBLOCKPOOL:
        storageclass = constants.DEFAULT_STORAGECLASS_RBD
    else:
        storageclass = constants.DEFAULT_STORAGECLASS_CEPHFS
    log.info(f"Using {storageclass} Storageclass")
    sf_data["spec"]["workload"]["args"]["storageclass"] = storageclass
    log.info("Running SmallFile bench")

    """
    Setting up the parameters for this test
    """
    sf_data["spec"]["workload"]["args"]["file_size"] = file_size
    sf_data["spec"]["workload"]["args"]["files"] = files
    sf_data["spec"]["workload"]["args"]["threads"] = threads
    sf_data["spec"]["workload"]["args"]["samples"] = samples
    """
    Calculating the size of the volume that need to be test, it should
    be at least twice in the size then the size of the files, and at
    least 100Gi.

    Since the file_size is in Kb and the vol_size need to be in Gb, more
    calculation is needed.
    """
    vol_size = int(files * threads * file_size * 3)
    vol_size = int(vol_size / constants.GB2KB)
    if vol_size < 100:
        vol_size = 100
    sf_data["spec"]["workload"]["args"]["storagesize"] = f"{vol_size}Gi"
    environment = get_environment_info()
    if not environment["user"] == "":
        sf_data["spec"]["test_user"] = environment["user"]
    else:
        # since full results object need this parameter, initialize it from CR file
        environment["user"] = sf_data["spec"]["test_user"]

    sf_data["spec"]["clustername"] = environment["clustername"]

    sf_obj = OCS(**sf_data)
    sf_obj.create()
    log.info(f"The smallfile yaml file is {sf_data}")

    # wait for benchmark pods to get created - takes a while
    for bench_pod in TimeoutSampler(
        240,
        10,
        get_pod_name_by_pattern,
        "smallfile-client",
        constants.RIPSAW_NAMESPACE,
    ):
        try:
            if bench_pod[0] is not None:
                small_file_client_pod = bench_pod[0]
                break
        except IndexError:
            log.info("Bench pod not ready yet")

    bench_pod = OCP(kind="pod", namespace=constants.RIPSAW_NAMESPACE)
    log.info("Waiting for SmallFile benchmark to Run")
    assert bench_pod.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_name=small_file_client_pod,
        sleep=30,
        timeout=600,
    )
    return backup_es, environment, sf_data
