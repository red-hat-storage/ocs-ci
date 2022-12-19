.PHONY: \
	deployment-odf\
	test-odf \
	deployment-ms-setup \
	test-ms \

deployment-odf:
	@echo "Deploy ODF Cluster"
	Docker_files/ocsci_container/scripts/deploy-odf-cluster.sh

test-odf:
	@echo "Running test on existing cluster"
	Docker_files/ocsci_container/scripts/test-odf.sh

deployment-ms-setup:
	@echo "Deploy Managed Service SetUp"
	Docker_files/ocsci_container/scripts/running-container.sh

test-ms:
	@echo "Running test on existing Managed Service SetUp"
	Docker_files/ocsci_container/scripts/running-container.sh
