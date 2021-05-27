chart_dir = "/stable/ibm-db2warehouse/ibm_cloud_pak/pak_extensions"
chart_git_url = "https://github.com/IBM/charts"
bdi_dir = "/tmp/bdi_temp_dir"
db2u_project = "db2u-project"
ldap_r_n = "test-bdi-db-release-name"
ldap_r_p = "test-bdi-bluadmin-release-password"
db2u_r_n = ldap_r_n
db2u_r_p = "test-bdi-db-release-password"
ibm_cloud_key = None
machine_set_replica = 3
db2u_pvc_name_suffix = "-db2u-meta-storage"

db2u_image_url = "icr.io/obs/hdm/db2u/db2u.db2client.workload:11.5.4.0-1362-x86_64"

temp_configure_dict = None
temp_data_load_dict = None
temp_run_dict = None
pvc_size = "200Gi"
scale_factor = 10
configure_timeout = 21600
data_load_timeout = 3600
run_workload_timeout = 18000


db2u_pvc_name = ldap_r_n + db2u_pvc_name_suffix

db2u_secret_name = db2u_r_n + "-db2u-instance"
temp_yaml_configure = None
temp_yaml_data = None
temp_yaml_run = None
