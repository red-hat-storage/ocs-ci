**

            CEPH CI Tests that are run using CentralCI(libcloud)
            
**

CEPH CI is a framework tightly coupled with CentralCI and Redhat Builds for
testing Ceph downstream builds with the CentralCI and Jenkins

It uses a modified version of Mita to create/destroy Ceph resources dynamically


There are various suites that are mapped to versions of the Ceph under test

eg:
  suites/sanity_ceph_deploy_13x will be valid for 1.3.x builds  
  suites/sanity_ceph_ansible that are valid for 2.0 builds
  
  
All suites are found inside the suites dir of cloned root dir, the current suites
are

sanity_ceph_ansible.yaml
sanity_ceph_deploy_13x.yaml
sanity_ceph_deploy.yaml
upgrade_13x_20.yaml

The tests inside the suites are described in yaml format

for eg:

```
tests:
   - test:
      name: ceph deploy
      module: test_ceph_deploy.py
      config:
        base_url: 'http://download-node-02.eng.bos.redhat.com/rcm-guest/ceph-drops/auto/ceph-1.3-rhel-7-compose/RHCEPH-1.3-RHEL-7-20161010.t.0/'
        installer_url: 
      desc: test cluster setup using ceph-deploy
      destroy-cluster: False
      abort-on-fail: True
      
   - test:
      name: rados workunit
      module: test_workunit.py
      config:
            test_name: rados/test_python.sh
            branch: hammer
      desc: Test rados python api
```

The above snippet describes 2 tests and the module is the name of the python
script that is executed to verify the test, every module can take a config
dict that is passed to it from the run wrapper, The run wrapper executes
the tests serially found in the suites. The test scripts are location in
the 'tests' folder of cloned root dir

Usage of run wrapper is:

A simple test suite wrapper that executes tests based on yaml test configuration

```
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  run.py --rhbuild BUILD --global-conf FILE --suite FILE [--use-cdn ]
        [--osp-cred <file>]
        [--rhs-con-repo <repo> --rhs-ceph-repo <repo>]
        [--add-repo <repo>]
        [--store]
        [--reuse <file>]
        [--skip-cluster]


Options:
  -h --help                         show this screen
  -v --version                      run version
  -s <smoke> --suite <smoke>        test suite to run
                                    eg: -s smoke or -s rbd
  -f <tests> --filter <tests>       filter tests based on the patter
                                    eg: -f 'rbd' will run tests that have 'rbd'
  --global-conf <file>              global configuration file
  --osp-cred <file>                 openstack credentials
  --rhbuild <1.3.0>                 ceph downstream version
                                    eg: 1.3.0, 2.0, 2.1 etc
  --use-cdn                         whether to use cdn or not [deafult: false]
  --rhs-con-repo <repo>             location of rhs console repo
                                    Top level location of console compose
  --rhs-ceph-repo <repo>            location of rhs-ceph repo
                                    Top level location of compose
  --add-repo <repo>                 Any additional repo's need to be enabled
  --store                           store the current vm state for reuse
  --reuse <file>                    use the stored vm state for rerun
  --skip-cluster                    skip cluster creation from ansible/ceph-deploy

```

global-conf describes the test bed configuration 
The image-name insde globals: define what image is used to clone ceph-nodes(
mon, osd, mds etc), The role maps to ceph role that the node will take
and osd generally attach 3 additional volumes with disk-size as specified in
config.

cloud-data file is important to setup the prerequisites on the node before
testing

```
globals:
    ceph-cluster:
       name: ceph
       create: true
       image-name: rhel-7.3-server-x86_64-latest
       vm-size: m1.medium
       node1:
         role: mon
       node2:
        role: osd
        no-of-volumes: 3
        disk-size: 20
       node3:
        role: osd
        no-of-volumes: 3
        disk-size: 20
       node4:
        role: osd
        no-of-volumes: 3
        disk-size: 20
       node5:
         role: client
       node6:
         role: ceph-installer
         image-name: rhel72_qa
    cloud-data: conf/cloud-data.yaml
```

osp-cred.yaml file has openstack credentials details to create/destroy resources.

```
globals:
    openstack-credentials:
        username: 'vakulkar'
        password: 'xxxxx'
        auth-url: 'http://10.8.188.11:5000'
        auth-version: '2.0_password'
        tenant-name: 'ceph-jenkins'
        service-region: 'regionOne'
        keypair: 'vasu-lp'
        
```

eg runs:

```
python run.py --rhbuild 2.1 --global-conf conf/sanity.yaml
                             --osp-cred conf/osp-cred.yaml
                            --suite suites/sanity_ceph_ansible.yaml


python run.py --rhbuild 2.1 --global-conf conf/sanity.yaml
     --osp-cred conf/osp-cred.yaml
     --suite suites/sanity_ceph_ansible.yaml
     --add-repo http://file.rdu.redhat.com/~kdreyer/scratch/rhscon-builds-for-rhceph-2.1/rhscon-builds-for-rhceph-2.1.repo
```