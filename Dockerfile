# set base image (host OS)
FROM centos:latest

RUN dnf install -y git openssl-devel python38 python3-devel python3-pip

# copy the project files to the working directory
#RUN git clone http://github.com/red-hat-storage/ocs-ci
COPY . /ocs-ci

# run setup
WORKDIR ocs-ci
RUN pip3 install --upgrade setuptools pip
RUN pip3 install -r requirements.txt

ENTRYPOINT ["run-ci"]



