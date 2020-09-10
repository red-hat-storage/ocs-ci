import configparser
import logging
import textwrap
import yaml

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import (
    config_to_string,
    get_system_architecture,
    update_container_with_mirrored_image
)

log = logging.getLogger(__name__)


def get_mcg_conf(mcg_obj, workload_bucket, custom_options=None):
    """
    Basic fio configuration for upgrade utilization for NooBaa S3 bucket.

    Args:
        mcg_obj (obj): MCG object, it can be found among fixtures
        workload_bucket (obj): MCG bucket
        custom_options (dict): Dictionary of lists containing tuples with
            additional configuration for fio in format:
            {'section': [('option', 'value'),...],...}
            e.g.
            {'global':[('name','bucketname')],'create':[('time_based','1'),('runtime','48h')]}
            Those values can be added to the config or rewrite already existing
            values

    Returns:
        str: updated fio configuration

    """
    config = configparser.ConfigParser()
    config.read_file(open(constants.FIO_S3))
    config.set('global', 'name', workload_bucket[0].name)
    config.set('global', 'http_s3_key', mcg_obj.access_key)
    config.set('global', 'http_s3_keyid', mcg_obj.access_key_id)
    if mcg_obj.s3_endpoint.startswith('https://'):
        http_host = mcg_obj.s3_endpoint[len('https://'):]
    elif mcg_obj.s3_endpoint.startswith('http://'):
        http_host = mcg_obj.s3_endpoint[len('http://'):]
    else:
        http_host = mcg_obj.s3_endpoint
    config.set(
        'global',
        'http_host',
        http_host.rstrip(':443')
    )
    config.set('global', 'http_s3_region', mcg_obj.region)
    config.set('global', 'filename', f"/{workload_bucket[0].name}/object")
    config.set('create', 'time_based', '1')
    config.set('create', 'runtime', '24h')

    # add or overwrite custom values
    if custom_options:
        for section in custom_options:
            for configuration in custom_options[section]:
                config.set(section, configuration[0], configuration[1])

    return config_to_string(config)


def get_pvc_dict():
    """
    PVC template for fio workloads.
    Note that all 'None' values needs to be defined before usage.

    Returns:
        dict: YAML data for a PVC object

    """
    template = textwrap.dedent("""
        kind: PersistentVolumeClaim
        apiVersion: v1
        metadata:
          name: fio-target
        spec:
          storageClassName: None
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: None
        """)
    pvc_dict = yaml.safe_load(template)
    return pvc_dict


def get_configmap_dict():
    """
    ConfigMap template for fio workloads.
    Note that you need to add actual configuration to workload.fio file.

    Returns:
        dict: YAML data for a OCP ConfigMap object

    """
    template = textwrap.dedent("""
        kind: ConfigMap
        apiVersion: v1
        metadata:
          name: fio-config
        data:
          workload.fio: |
            # here comes workload configuration
        """)
    cm_dict = yaml.safe_load(template)
    return cm_dict


def get_job_dict():
    """
    Job template for fio workloads.

    Returns:
        dict: YAML data for a job object

    """
    arch = get_system_architecture()
    if arch.startswith('x86'):
        image = 'quay.io/fbalak/fio-fedora:latest'
    else:
        image = 'quay.io/multiarch-origin-e2e/fio-fedora:latest'

    log.info(f'Discovered architecture: {arch.strip()}')
    log.info(f'Using image: {image}')

    # TODO(fbalak): load dictionary fixtures from one place
    template = textwrap.dedent(f"""
        apiVersion: batch/v1
        kind: Job
        metadata:
          name: fio
        spec:
          backoffLimit: 0
          template:
            metadata:
              name: fio
            spec:
              containers:
                - name: fio
                  image: {image}
                  command:
                    - "/usr/bin/fio"
                    - "--output-format=json"
                    - "/etc/fio/workload.fio"
                  volumeMounts:
                    - name: fio-target
                      mountPath: /mnt/target
                    - name: fio-config-volume
                      mountPath: /etc/fio
              restartPolicy: Never
              volumes:
                - name: fio-target
                  persistentVolumeClaim:
                    claimName: fio-target
                - name: fio-config-volume
                  configMap:
                    name: fio-config
        """)
    job_dict = yaml.safe_load(template)

    # overwrite used image (required for disconnected installation)
    update_container_with_mirrored_image(job_dict)

    return job_dict
