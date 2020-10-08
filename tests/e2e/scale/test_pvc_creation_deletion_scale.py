"""
Test to measure pvc scale creation & deletion time. Total pvc count would be 1500
"""
import logging
import csv
import pytest
import threading

from tests import helpers
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.framework.testlib import scale, E2ETest, polarion_id
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile

log = logging.getLogger(__name__)


@scale
class TestPVCCreationDeletionScale(E2ETest):
    """
    Base class for PVC scale creation and deletion
    """
    @pytest.fixture()
    def namespace(self, project_factory):
        """
        Create a new project
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

    @pytest.mark.parametrize(
        argnames=["access_mode", "interface"],
        argvalues=[
            pytest.param(
                *[constants.ACCESS_MODE_RWO, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-1225")
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2010")
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHFS_INTERFACE],
                marks=pytest.mark.polarion_id("OCS-2008")
            ),
        ]
    )
    @pytest.mark.usefixtures(namespace.__name__)
    def test_multiple_pvc_creation_deletion_scale(self, namespace, tmp_path, access_mode, interface):
        """
        Measuring PVC creation time while scaling PVC
        Measure PVC deletion time after creation test
        """
        number_of_pvc = 750
        log.info(f"Start creating {access_mode}-{interface} {number_of_pvc} PVC")
        if interface == constants.CEPHBLOCKPOOL:
            sc_name = constants.DEFAULT_STORAGECLASS_RBD
        elif interface == constants.CEPHFS_INTERFACE:
            sc_name = constants.DEFAULT_STORAGECLASS_CEPHFS

        # Get pvc_dict_list, append all the pvc.yaml dict to pvc_dict_list
        pvc_dict_list1 = scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=number_of_pvc, access_mode=access_mode, sc_name=sc_name
        )
        pvc_dict_list2 = scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=number_of_pvc, access_mode=access_mode, sc_name=sc_name
        )

        # There is 2 kube_job to reduce the load, observed time_out problems
        # during delete process of single kube_job and heavy load.
        job_file1 = ObjectConfFile(
            name='job_profile_1', obj_dict_list=pvc_dict_list1,
            project=self.namespace, tmp_path=tmp_path
        )
        job_file2 = ObjectConfFile(
            name='job_profile_2', obj_dict_list=pvc_dict_list2,
            project=self.namespace, tmp_path=tmp_path
        )

        # Create kube_job
        job_file1.create(namespace=self.namespace)
        job_file2.create(namespace=self.namespace)

        # Check all the PVC reached Bound state
        pvc_bound_list = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
            kube_job_obj=job_file1, namespace=self.namespace, no_of_pvc=number_of_pvc
        )
        pvc_bound_list.extend(
            scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
                kube_job_obj=job_file2, namespace=self.namespace, no_of_pvc=number_of_pvc
            )
        )

        logging.info(f"Length of pvc_bound_list {len(pvc_bound_list)}")

        # Get PVC creation time
        pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=interface, pvc_name_list=pvc_bound_list
        )

        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        log_path = f"{ocsci_log_path()}/{interface}-{access_mode}"
        with open(f"{log_path}-creation-time.csv", "w") as fd:
            csv_obj = csv.writer(fd)
            for k, v in pvc_create_time.items():
                csv_obj.writerow([k, v])
        logging.info(
            f"Create data present in {log_path}-creation-time.csv file"
        )

        # Get pv_name, require pv_name to fetch deletion time data from log
        # TODO: Revisit on changing below code without threads, for now it looks good
        # TODO: but run-ci memory growth is increasing ~0.4G with below threads
        pvc_objs = pvc.get_all_pvc_objs(namespace=self.namespace)
        threads, pv_name_list = ([] for i in range(2))
        for pvc_obj in pvc_objs:
            process1 = threading.Thread(target=pvc_obj.reload)
            process2 = threading.Thread(target=pv_name_list.append(pvc_obj.backed_pv))
            process1.start()
            process2.start()
            threads.append(process1)
            threads.append(process2)
        for process in threads:
            process.join()

        # Delete kube_job
        job_file1.delete(namespace=self.namespace)
        job_file2.delete(namespace=self.namespace)

        # Get PVC deletion time
        pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
            interface=interface, pv_name_list=pv_name_list
        )

        # Update result to csv file.
        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        with open(f"{log_path}-deletion-time.csv", "w") as fd:
            csv_obj = csv.writer(fd)
            for k, v in pvc_deletion_time.items():
                csv_obj.writerow([k, v])
        logging.info(
            f"Delete data present in {log_path}-deletion-time.csv file"
        )

    @polarion_id('OCS-1885')
    @pytest.mark.usefixtures(namespace.__name__)
    def test_all_4_type_pvc_creation_deletion_scale(self, namespace, tmp_path):
        """
        Measuring PVC creation time while scaling PVC of all 4 types, Total 1500 PVCs
        will be created, i.e. 375 each pvc type
        Measure PVC deletion time in scale env
        """
        log.info("Start creating 1500 PVC of all 4 types")
        cephfs_sc_obj = constants.DEFAULT_STORAGECLASS_CEPHFS
        rbd_sc_obj = constants.DEFAULT_STORAGECLASS_RBD

        # Get pvc_dict_list, append all the pvc.yaml dict to pvc_dict_list
        rbd_pvc_dict_list, cephfs_pvc_dict_list = ([] for i in range(2))
        for mode in [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]:
            rbd_pvc_dict_list.extend(
                scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
                    no_of_pvc=375, access_mode=mode, sc_name=rbd_sc_obj
                )
            )
            cephfs_pvc_dict_list.extend(
                scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
                    no_of_pvc=375, access_mode=mode, sc_name=cephfs_sc_obj
                )
            )

        # There is 2 kube_job for cephfs and rbd PVCs
        job_file_rbd = ObjectConfFile(
            name='rbd_pvc_job', obj_dict_list=rbd_pvc_dict_list,
            project=self.namespace, tmp_path=tmp_path
        )
        job_file_cephfs = ObjectConfFile(
            name='cephfs_pvc_job', obj_dict_list=cephfs_pvc_dict_list,
            project=self.namespace, tmp_path=tmp_path
        )

        # Create kube_job
        job_file_rbd.create(namespace=self.namespace)
        job_file_cephfs.create(namespace=self.namespace)

        # Check all the PVC reached Bound state
        rbd_pvc_name = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
            kube_job_obj=job_file_rbd, namespace=self.namespace, no_of_pvc=750
        )
        fs_pvc_name = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
            kube_job_obj=job_file_cephfs, namespace=self.namespace, no_of_pvc=750
        )

        # Get pvc objs from namespace, which is used to identify backend pv
        rbd_pvc_obj, cephfs_pvc_obj = ([] for i in range(2))
        pvc_objs = pvc.get_all_pvc_objs(namespace=self.namespace)
        for pvc_obj in pvc_objs:
            if pvc_obj.backed_sc == constants.DEFAULT_STORAGECLASS_RBD:
                rbd_pvc_obj.append(pvc_obj)
            elif pvc_obj.backed_sc == constants.DEFAULT_STORAGECLASS_CEPHFS:
                cephfs_pvc_obj.append(pvc_obj)

        # Get PVC creation time
        fs_pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=constants.CEPHFS_INTERFACE, pvc_name_list=fs_pvc_name
        )
        rbd_pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=constants.CEPHBLOCKPOOL, pvc_name_list=rbd_pvc_name
        )
        fs_pvc_create_time.update(rbd_pvc_create_time)

        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        log_path = f"{ocsci_log_path()}/All-type-PVC"
        with open(f"{log_path}-creation-time.csv", "w") as fd:
            csv_obj = csv.writer(fd)
            for k, v in fs_pvc_create_time.items():
                csv_obj.writerow([k, v])
        logging.info(
            f"Create data present in {log_path}-creation-time.csv file"
        )

        # Get pv_name, require pv_name to fetch deletion time data from log
        # TODO: Revisit on changing below code without threads, for now it looks good
        # TODO: but run-ci memory growth is increasing ~0.4G with below threads
        fs_pv_list, rbd_pv_list = ([] for i in range(2))
        threads = list()
        for fs_obj, rbd_obj in zip(cephfs_pvc_obj, rbd_pvc_obj):
            process1 = threading.Thread(target=fs_obj.reload)
            process2 = threading.Thread(target=rbd_obj.reload)
            process3 = threading.Thread(target=fs_pv_list.append(fs_obj.backed_pv))
            process4 = threading.Thread(target=rbd_pv_list.append(rbd_obj.backed_pv))
            process1.start()
            process2.start()
            process3.start()
            process4.start()
            threads.append(process1)
            threads.append(process2)
            threads.append(process3)
            threads.append(process4)
        for process in threads:
            process.join()

        # Delete kube_job
        job_file_rbd.delete(namespace=self.namespace)
        job_file_cephfs.delete(namespace=self.namespace)

        # Get PV deletion time
        fs_pvc_deletion_time = helpers. measure_pv_deletion_time_bulk(
            interface=constants.CEPHFS_INTERFACE, pv_name_list=fs_pv_list
        )
        rbd_pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
            interface=constants.CEPHBLOCKPOOL, pv_name_list=rbd_pv_list
        )
        fs_pvc_deletion_time.update(rbd_pvc_deletion_time)

        # TODO: Update below code with google API, to record value in spreadsheet
        # TODO: For now observing Google API limit to write more than 100 writes
        with open(f"{log_path}-deletion-time.csv", "w") as fd:
            csv_obj = csv.writer(fd)
            for k, v in fs_pvc_deletion_time.items():
                csv_obj.writerow([k, v])
        logging.info(
            f"Delete data present in {log_path}-deletion-time.csv file"
        )
