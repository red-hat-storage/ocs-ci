// This Jenkinsfile is intended to be used with a companion jenkins-job-builder
// definition. It requires the following parameters:
//   AWS_DOMAIN
//   AWS_REGION
//   CLUSTER_USER
// It also requires credentials with these IDs to be present in the CI system:
//   openshift-dev-aws-access-key-id (AWS_ACCESS_KEY_ID)
//   openshift-dev-aws-secret-access-key (AWS_SECRET_ACCESS_KEY)
//   openshift-pull-secret (PULL_SECRET)
//   ocs-bugzilla-cfg (BUGZILLA_CFG)
// It may also provide these optional parameters to override the framework's
// defaults:
//   OCS_OPERATOR_DEPLOYMENT
//   OCS_OPERATOR_IMAGE
//   ROOK_IMAGE
//   CEPH_IMAGE
//   CEPH_CSI_IMAGE
//   ROOK_CSI_REGISTRAR_IMAGE
//   ROOK_CSI_PROVISIONER_IMAGE
//   ROOK_CSI_SNAPSHOTTER_IMAGE
//   ROOK_CSI_ATTACHER_IMAGE
//   EMAIL
//   UMB_MESSAGE
import groovy.json.JsonBuilder
pipeline {
  agent { node { label "ocs-ci" }}
  environment {
    AWS_SHARED_CREDENTIALS_FILE = "${env.WORKSPACE}/.aws/credentials"
    AWS_CONFIG_FILE = "${env.WORKSPACE}/.aws/config"
    AWS_ACCESS_KEY_ID = credentials('openshift-dev-aws-access-key-id')
    AWS_SECRET_ACCESS_KEY = credentials('openshift-dev-aws-secret-access-key')
    PULL_SECRET = credentials('openshift-pull-secret')
    BUGZILLA_CFG = credentials('ocs-bugzilla-cfg')
  }
  stages {
    stage("Setup") {
      steps {
        sh """
          if [ ! -z '${env.EMAIL}' ]; then
            sudo yum install -y /usr/sbin/postfix
            sudo systemctl start postfix
          fi
          sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1
          sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1
          python3 -V
          pip3 install --user virtualenv
          python3 -m virtualenv venv
          source ./venv/bin/activate
          pip3 install tox
          pip3 install -r requirements.txt
          python3 setup.py develop
          python3 ./.functional_ci_setup.py --skip-aws
          """
      }
    }
    stage("Lint") {
      steps {
        sh """
          source ./venv/bin/activate
          tox -e flake8
          """
      }
    }
    stage("Unit test") {
      steps {
        sh """
          source ./venv/bin/activate
          tox -e py36
          """
      }
    }
    stage("Deploy OCP") {
      steps {
        sh """
        source ./venv/bin/activate
        run-ci -m deployment --deploy --ocsci-conf=ocs-ci-ocp.yaml --cluster-name=${env.CLUSTER_USER}-ocs-ci-${env.BUILD_ID} --cluster-path=cluster --collect-logs
        """
      }
    }
    stage("Deploy OCS") {
      steps {
        sh """
        source ./venv/bin/activate
        run-ci -m deployment --deploy --ocsci-conf=ocs-ci-ocs.yaml --cluster-name=${env.CLUSTER_USER}-ocs-ci-${env.BUILD_ID} --cluster-path=cluster --collect-logs
        """
      }
    }
    stage("Tier 1") {
      environment {
        EMAIL_ARG = """${sh(
          returnStdout: true,
          script: "if [ ! -z '${env.EMAIL}' ]; then echo -n '--email=${env.EMAIL}'; fi"
        )}"""
      }
      steps {
        sh """
        source ./venv/bin/activate
        run-ci -m tier1 --ocsci-conf=ocs-ci-ocs.yaml --cluster-name=${env.CLUSTER_USER}-ocs-ci-${env.BUILD_ID} --cluster-path=cluster --self-contained-html --html=${env.WORKSPACE}/logs/report.html --junit-xml=${env.WORKSPACE}/logs/junit.xml --collect-logs ${env.EMAIL_ARG}
        """
      }
    }
  }
  post {
    always {
      archiveArtifacts artifacts: 'ocs-ci-*.yaml,cluster/**,logs/**', fingerprint: true
      sh """
        source ./venv/bin/activate
        run-ci -m deployment --teardown --ocsci-conf=ocs-ci-ocs.yaml --cluster-name=${env.CLUSTER_USER}-ocs-ci-${env.BUILD_ID} --cluster-path=cluster --collect-logs
        """
      junit testResults: "logs/junit.xml", keepLongStdio: false
    }
    success {
      script {
        if( env.UMB_MESSAGE in [true, 'true'] ) {
          def properties = '''
            TOOL=ocs-ci
            PRODUCT=ocs
            PRODUCT_BUILD_CAUSE=${BUILD_CAUSE}
            OCS_OPERATOR_DEPLOYMENT=${env.OCS_OPERATOR_DEPLOYMENT}
          '''
          def contentObj = [
            "SENDER_BUILD_NUMBER": "${BUILD_NUMBER}",
            "OCS_OPERATOR_IMAGE": "${env.OCS_OPERATOR_IMAGE}",
            "ROOK_IMAGE": "${ROOK_IMAGE}",
            "CEPH_IMAGE": "${CEPH_IMAGE}",
            "CEPH_CSI_IMAGE": "${CEPH_CSI_IMAGE}",
            "ROOK_CSI_REGISTRAR_IMAGE": "${ROOK_CSI_REGISTRAR_IMAGE}",
            "ROOK_CSI_PROVISIONER_IMAGE": "${ROOK_CSI_PROVISIONER_IMAGE}",
            "ROOK_CSI_SNAPSHOTTER_IMAGE": "${ROOK_CSI_SNAPSHOTTER_IMAGE}",
            "ROOK_CSI_ATTACHER_IMAGE": "${ROOK_CSI_ATTACHER_IMAGE}",
          ]
          def builder = new JsonBuilder()
          builder(contentObj)
          def content = builder.toString()
          echo "Sending UMB message"
          echo "Properties: %s".format(properties)
          echo "Content: %s".format(content)
          sendCIMessage (
            providerName: 'Red Hat UMB',
            overrides: [ topic: 'VirtualTopic.qe.ci.jenkins' ],
            failOnError: false,
            messageType: 'Tier1TestingDone',
            messageProperties: properties,
            messageContent: content
          )
        }
      }
    }
  }
}
