#!/usr/bin/env bash

#################################################################################
#                                                                               #
# This script is for building generic performance container and push it to      #
# repository. it used the 'docker' for the build and accept the build TAG,      #
# the repo to push into, a dockerfile and the ability to build multi-arch image #
#                                                                               #
#################################################################################

# Build multi-arch image - default is no (0)
Multi=0

# List of architecture for the multi-arch build
Platforms="linux/amd64,linux/ppc64le,linux/s390x,linux/arm64"

# The tool to use for the build.
# using docker since podman doesn't not support the multi-arch
CMD_TOOL="podman"

usage() {
  # Display the script usage and exit the script
	echo "Usage: $0 -f <filename> -r <repo name> [-t <image tag>] [-m] [-h]"
	echo "  -f <filename>  : the filename which used for building"
	echo "  -t <image tag> : the tagging of the image - default is 'latest'"
	echo "  -r <repo name> : full repository name including registry site"
	echo "                   e.g. : docker.io/<username>/<repo name>"
	echo "  -m             : create multi-arch image - default is x86_64 only"
	echo "                   multi-arch that avaliable are : x86_64 / ppc64le / s390x"
	echo "  -h             : display this screen"

	exit 1
}


# parsing the command line parameters
while getopts "t:r:f:m" o; do
    case "${o}" in
        f)
            Dockerfile=${OPTARG}
            ;;
        t)
            Tag=${OPTARG}
            ;;
        r)
            Repo=${OPTARG}
            ;;
	      m)
	          Multi=1
	          ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

# Validate that image repository provided
if [[ ${Repo} == "" ]] ; then
	echo "Error: you mast give the Base path for the repository!"
	usage
fi

# validate the image tag, if not exist, use 'latest'
if [[ ${Tag} == "" ]] ; then
	echo "no tag provided, going to use 'latest'"
	Tag='latest'
fi

# Validate that Dockerfile is provided
if [[ ${Dockerfile} == "" ]] ; then
  echo "No dockerfile supplied, using 'Dockerfile' in current dir"
  Dockerfile='Dockerfile'

fi

# Validate that the Dockerfile is exist
if [[ ! -f ${Dockerfile} ]] ; then
  echo "Error : Dockerfile does not exist !"
  exit 2
fi

# Creating the appropriate build command (single/multi arch)
if [[ ${Multi} -eq 0 ]] ; then
	echo "Building the image for x86_64 Arch only"
	CMD="${CMD_TOOL} build --tag ${Repo}:${Tag} --file ${Dockerfile} ."
else
	echo "Building the image for Multi Arch"
	CMD="${CMD_TOOL} buildx build --platform ${Platforms} --tag ${Repo}:${Tag} --file ${Dockerfile} ."
fi

# Run the build command
echo "Going to run : ${CMD}"
${CMD}

# Push the image - for single arch only
${CMD_TOOL} push ${Repo}:${Tag}
