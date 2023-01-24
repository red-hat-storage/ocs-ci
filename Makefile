.PHONY: \
	build-image \
	run-ocs-ci \

build-image:
	@echo "Build OCS-CI image"
	Docker_files/ocsci_container/build-image.sh

run-ocs-ci:
	@echo "Running OCS-CI"
	Docker_files/ocsci_container/scripts/run-script.sh
