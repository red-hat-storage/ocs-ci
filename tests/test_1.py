from tests import helpers


class Test1:
    def test_1(self):
        project_name = helpers.create_unique_resource_name('test', 'project')
        helpers.create_project(project_name)

        cbp_name = helpers.create_unique_resource_name('test', 'cephblockpool')
        helpers.create_ceph_block_pool(cbp_name, project_name)

        sc_name = helpers.create_unique_resource_name('test', 'storageclass')
        helpers.create_storageclass(sc_name, project_name, cbp_name=cbp_name)

        pvc_name = helpers.create_unique_resource_name('test', 'pvc')
        helpers.create_pvc(pvc_name, project_name)

        pod_name = helpers.create_unique_resource_name('test', 'pod')
        helpers.create_pod(pod_name, project_name, pvc_name=pvc_name)

        helpers.delete_pod(pod_name, project_name)

        helpers.delete_pvc(pvc_name, project_name)

        helpers.delete_storage_class(sc_name, project_name)

        helpers.delete_ceph_block_pool(cbp_name, project_name)

        helpers.delete_project(project_name)
