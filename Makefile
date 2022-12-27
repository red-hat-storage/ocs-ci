.PHONY: \
	run-ocs-ci\

run-ocs-ci:
	@echo "Running OCS-CI"
	Docker_files/ocsci_container/scripts/run-script.sh
