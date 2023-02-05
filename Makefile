.PHONY: \
	build-image \
	run-ocs-ci \

build-image:
	@echo "Build OCS-CI image"
	Docker_files/ocsci_container/build-image.sh

run-managed-service:
	@echo "Running managed-service"
	Docker_files/ocsci_container/scripts/run-managed-service.sh

run-odf:
	@echo "Running OCS-CI"
	Docker_files/ocsci_container/scripts/run-odf.sh

debug-odf:
	@echo "Debug OCS-CI"
	Docker_files/ocsci_container/scripts/debug-odf.sh
