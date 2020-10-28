// This Jenkinsfile is intended to be used with a companion jenkins-job-builder
// definition. It requires the following parameters:
//   AWS_DOMAIN
//   AWS_REGION
//   CLUSTER_USER
// It also requires credentials with these IDs to be present in the CI system.
// The openshift aws credentials are stored per user so that we can easily
// switch to another user's credentials if need be:
//   openshift-dev-aws-access-key-id-$CLUSTER_USER (AWS_ACCESS_KEY_ID)
//   openshift-dev-aws-secret-access-key-$CLUSTER_USER (AWS_SECRET_ACCESS_KEY)
//   openshift-pull-secret (PULL_SECRET)
//   ocs-bugzilla-cfg (BUGZILLA_CFG)
// It may also provide these optional parameters to override the framework's
// defaults:
//   OCS_REGISTRY_IMAGE
//   EMAIL
//   UMB_MESSAGE
//   DEBUG_CLUSTER
def LAST_STAGE

pipeline {
  agent { node { label "ocs-ci" }}
  environment {
    AWS_SHARED_CREDENTIALS_FILE = "${env.WORKSPACE}/.aws/credentials"
    AWS_CONFIG_FILE = "${env.WORKSPACE}/.aws/config"
    AWS_ACCESS_KEY_ID = credentials("openshift-dev-aws-access-key-id-${env.CLUSTER_USER}")
    AWS_SECRET_ACCESS_KEY = credentials("openshift-dev-aws-secret-access-key-${env.CLUSTER_USER}")
    PULL_SECRET = credentials('openshift-pull-secret')
    BUGZILLA_CFG = credentials('ocs-bugzilla-cfg')
  }
  stages {
    stage("Setup") {
      steps {
        script { LAST_STAGE=env.STAGE_NAME }
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
        script { LAST_STAGE=env.STAGE_NAME }
        sh """
          source ./venv/bin/activate
          tox -e flake8
          """
      }
    }
    stage("Unit test") {
      steps {
        script { LAST_STAGE=env.STAGE_NAME }
        sh """
          source ./venv/bin/activate
          tox -e py36
          """
      }
    }
    stage("Deploy OCP") {
      steps {
        script { LAST_STAGE=env.STAGE_NAME }
        sh """
        source ./venv/bin/activate
        run-ci -m deployment --deploy --ocsci-conf=ocs-ci-ocp.yaml --ocsci-conf=conf/ocsci/production-aws-ipi.yaml --ocsci-conf=conf/ocsci/production_device_size.yaml --cluster-name=${env.CLUSTER_USER}-ocsci-${env.BUILD_ID} --cluster-path=cluster --collect-logs
        """
      }
    }
    stage("Deploy OCS") {
      steps {
        script { LAST_STAGE=env.STAGE_NAME }
        sh """
        source ./venv/bin/activate
        run-ci -m deployment --deploy --ocsci-conf=ocs-ci-ocs.yaml --ocsci-conf=conf/ocsci/production-aws-ipi.yaml --cluster-name=${env.CLUSTER_USER}-ocsci-${env.BUILD_ID} --cluster-path=cluster --collect-logs
        """
      }
    }
    stage("Acceptance Tests") {
      environment {
        EMAIL_ARG = """${sh(
          returnStdout: true,
          script: "if [ ! -z '${env.EMAIL}' ]; then echo -n '--email=${env.EMAIL}'; fi"
        )}"""
      }
      steps {
        script { LAST_STAGE=env.STAGE_NAME }
        sh """
        source ./venv/bin/activate
        run-ci -m acceptance --ocsci-conf=ocs-ci-ocs.yaml --cluster-name=${env.CLUSTER_USER}-ocsci-${env.BUILD_ID} --cluster-path=cluster --self-contained-html --html=${env.WORKSPACE}/logs/report.html --junit-xml=${env.WORKSPACE}/logs/junit.xml --collect-logs --bugzilla ${env.EMAIL_ARG}
        """
      }
    }
  }
  post {
    always {
      archiveArtifacts artifacts: 'ocs-ci-*.yaml,cluster/**,logs/**', fingerprint: true
      script {
        if( env.DEBUG_CLUSTER in [true, 'true'] ) {
          input(message: "Proceed with cluster teardown?")
        }
      }
      sh """
        source ./venv/bin/activate
        run-ci -m deployment --teardown --ocsci-conf=ocs-ci-ocs.yaml --cluster-name=${env.CLUSTER_USER}-ocsci-${env.BUILD_ID} --cluster-path=cluster --collect-logs
        """
      junit testResults: "logs/junit.xml", keepLongStdio: false
    }
    failure {
      script {
        if ( LAST_STAGE != "Acceptance Tests" ) {
          emailext (
            subject: "Job '${env.JOB_NAME}' build #${env.BUILD_ID} failed during stage '${LAST_STAGE}'",
            body: "Build failed : ${env.BUILD_URL}",
            from: "${env.EMAIL}",
            to: "${env.EMAIL}"
          )
        }
      }
    }
    success {
      script {
        def registry_image = "${env.OCS_REGISTRY_IMAGE}"
        // quay.io/rhceph-dev/ocs-registry:4.2.2-255.ci -> 4.2.2-255.ci
        def registry_tag = registry_image.split(':')[-1]
        def ocs_version = registry_tag.split('-')[0]
        // ocs version with only X.Y
        def short_ocs_version = ocs_version.tokenize(".").take(2).join(".")
        if (short_ocs_version.toFloat() < 4.5 ) {
          // tag ocs-olm-operator container because versions < 4.5 use olm registry containers
          build job: 'quay-tag-image', parameters: [string(name: "SOURCE_URL", value: "${registry_image}"), string(name: "QUAY_IMAGE_TAG", value: "ocs-olm-operator:latest-stable-${ocs_version}")]
          build job: 'quay-tag-image', parameters: [string(name: "SOURCE_URL", value: "${registry_image}"), string(name: "QUAY_IMAGE_TAG", value: "ocs-olm-operator:latest-stable-${short_ocs_version}")]
        }
        else {
          // tag ocs-operator-bundle and ocs-registry container because versions > 4.5 use bundle index containers
          build job: 'quay-tag-image', parameters: [string(name: "SOURCE_URL", value: "${registry_image}"), string(name: "QUAY_IMAGE_TAG", value: "ocs-operator-bundle:latest-stable-${ocs_version}")]
          build job: 'quay-tag-image', parameters: [string(name: "SOURCE_URL", value: "${registry_image}"), string(name: "QUAY_IMAGE_TAG", value: "ocs-operator-bundle:latest-stable-${short_ocs_version}")]
          build job: 'quay-tag-image', parameters: [string(name: "SOURCE_URL", value: "${registry_image}"), string(name: "QUAY_IMAGE_TAG", value: "ocs-registry:latest-stable-${ocs_version}")]
          build job: 'quay-tag-image', parameters: [string(name: "SOURCE_URL", value: "${registry_image}"), string(name: "QUAY_IMAGE_TAG", value: "ocs-registry:latest-stable-${short_ocs_version}")]
        }
        if( env.UMB_MESSAGE in [true, 'true'] ) {
          def registry_version = registry_tag.split('-')[0]
          def properties = """
            TOOL=ocs-ci
            PRODUCT=ocs
            PRODUCT_VERSION=${registry_version}
          """
          def content_string = """{
            "SENDER_BUILD_NUMBER": "${BUILD_NUMBER}",
            "OCS_REGISTRY_IMAGE": "${env.OCS_REGISTRY_IMAGE}",
          }"""
          def content = readJSON text: content_string
          echo "Sending UMB message"
          echo 'Properties: ' + properties
          echo 'Content: ' + content.toString()
          sendCIMessage (
            providerName: 'Red Hat UMB',
            overrides: [ topic: 'VirtualTopic.qe.ci.jenkins' ],
            failOnError: false,
            messageType: 'ProductAcceptedForReleaseTesting',
            messageProperties: properties,
            messageContent: content.toString()
          )
        }
      }
    }
  }
}
