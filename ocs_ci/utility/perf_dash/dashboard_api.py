"""
API module to interact with the Performance Dashboard
In order to push results into the dashboard, and pulling data
from it to compare between results.

    The DB structure is :

        some tables which use for indexing values and all in the same structure:

        versions : list of all OCS versions which tested
        platform : list of tested platform (e.g. AWS / VSphere etc.)
        az_topology : Topology of the tested environment (e.g. 3-AZ)
        tests : Tests name which ran (e.g. FIO / SmallFiles etc.)

        table name :
            ID (int) : the index - unique number
            Name (str) : the name to index

        the `builds` table contain the builds of a particular OCS version

        table name : builds
            ID (int) : the index - unique number
            version (int) : version ID - must be exists in the versions table
            Name (str) : the name (or number) of the build (e.g. 254 / RC1-312 / GA)

        the `results` table contain the complete data about individual test

        table name : results
            ID (int): the index - unique number
            sample (int): number of the sample - each test can be run more the one time
            version (int): version ID - must be exists in the versions table
            build (int): build ID - must be exists in the builds table
            platform (int): platform ID - must be exists in the platform table
            az_topology (int): topology ID - must be exists in the az_topology table
            test_name (int): test ID - must be exists in the tests table
            es_link (str): the elasticsearch links to the individual test separated by comma
            e.g. http://<es_server>:<port>/<sub-test_1>, http://<es_server>:<port>/<sub-test_2>
            log_file (str): link to full test log

        One single test test (e.g. FIO) can be split to few sub-tests (by parametrize),
        e.g.  : [CEPHBLOCKPOOL, sequential], [CEPHFILESYSTEM, sequential],
        [CEPHBLOCKPOOL, random], [CEPHFILESYSTEM, random]

        but in the dashboard all those tests (4 in the FIO example) are displayed as single test

"""
# Builtin modules
import logging

# 3ed party modules
import mysql.connector

# Local modules
from ocs_ci.framework import config
from ocs_ci.ocs import exceptions

log = logging.getLogger(__name__)


class PerfDash(object):
    """
    The API class to connect and managing the performance dashboard database
    """

    def __init__(self):
        """
        Initializing the dashboard object and make a connection

        Raise:
            if credential file can not be open / read
            if the connection failed

        """

        # Reading connection information and credentials from local file
        # which is not stored in the GitHub repository
        self.creds = config.AUTH.get("perf_dashboard", None)
        if self.creds is None:
            log.error("Dashboard credentials are not defined in configuration")
            raise exceptions.MissingRequiredConfigKeyError(
                "The key AUTH:perf_dashboard is missing in configuration file"
            )

        self.creds["raise_on_warnings"] = True
        self.creds["use_pure"] = True

        # Connecting to the Dashboard DB
        self.connect()

    def connect(self):
        """
        Create DB connection

        Raise:
            in case of connection failed - use the exception that caught

        """
        log.info("Creating DB connector and connect to it")
        try:
            self.cnx = mysql.connector.connect(**self.creds)
            self.cursor = self.cnx.cursor()
        except Exception as err:
            log.error(f"Can not connect to DB - [{err}]")
            raise err

    def _run_query(self, query=None):
        """
        Run an SQL Query

        Args:
            query (str): sql query string

        Returns:
            bool: True if succeed, otherwise False
        """
        log.debug(f"Try to Execute query : {query}")
        try:
            self.cursor.execute(query)
            return True
        except Exception as err:
            log.error(f"Can not execute [{query}]\n{err}")
            return False

    def get_id_by_name(self, table=None, name=None):
        """
        Query the ID of specific 'name' value from selected table

        Args:
            table (str): The table from which data will be query
            name (str): The value for the 'name' field in the table to query

        Returns:
             int : the value of the ID field in the table, otherwise None

        """
        query = f"SELECT id FROM {table} WHERE name = '{name}' ;"
        if self._run_query(query=query):
            for result_id in self.cursor:
                # The query return a tuple and we need only the firs element
                return result_id[0]

        return None

    def get_name_by_id(self, table=None, recid=None):
        """
        Query the name of specific 'id' value from selected table

        Args:
            table (str): The table from which data will be query
            recid (int): The value for the 'id' field in the table to query

        Returns:
             str : the value of the Name field in the table, otherwise None

        """
        query = f"SELECT name FROM {table} WHERE id = {recid} ;"
        if self._run_query(query=query):
            for result_name in self.cursor:
                # The query return a tuple and we need only the first element
                return result_name[0]

        return None

    def insert_single_value(self, table=None, value=None):
        """
        Insert a value to 'table' and return it's ID

        Args:
            table (str): The table to insert data into
            value (str): The value to insert

        Returns:
             int : the ID of the value in the table, otherwise None

        """
        # prevent of duplicate records for the same build
        record_id = self.get_id_by_name(table=table, name=value)
        if record_id is not None:
            return record_id

        query = f"INSERT INTO {table} (name) VALUES ('{value}') ;"
        if self._run_query(query=query):
            try:
                rec_id = self.cursor.lastrowid
                # Make sure data is committed to the database
                self.cnx.commit()
                return rec_id
            except Exception as err:
                log.error(f"Can not insert {value} into {table} - [{err}]")
                return None

    def get_version_id(self, version=None):
        """
        Query of the ID of version number in the DB

        Args:
            version (str): The version number (e.g. 4.9.0)

        Returns:
             int : the version ID in the DB

        """
        return self.get_id_by_name("versions", version)

    def get_test_id(self, test=None):
        """
        Query of the ID of test name in the DB

        Args:
            test (str): The test name (e.g. FIO)

        Returns:
             int : the Test ID in the DB

        """
        return self.get_id_by_name("tests", test)

    def get_platform_id(self, platform=None):
        """
        Query of the ID of platform name in the DB

        Args:
            platform (str): The platform name (e.g. AWS)

        Returns:
             int : the Platform ID in the DB

        """
        return self.get_id_by_name("platform", platform)

    def get_topology_id(self, topology=None):
        """
        Query of the ID of platform name in the DB

        Args:
            topology (str): The Topology name (e.g. 3-AZ)

        Returns:
             int : the Topology ID in the DB

        """
        return self.get_id_by_name("az_topology", topology)

    def get_version_name(self, version=0):
        """
        Query of the Name of version ID in the DB

        Args:
            version (int): The version ID

        Returns:
             str : the version name in the DB

        """
        return self.get_name_by_id("versions", version)

    def get_test_name(self, test=0):
        """
        Query of the Name of test ID in the DB

        Args:
            test (int): The test ID

        Returns:
             str : the Test Name in the DB

        """
        return self.get_name_by_id("tests", test)

    def get_platform_name(self, platform=0):
        """
        Query of the IName of platform ID in the DB

        Args:
            platform (int): The platform ID

        Returns:
             str : the Platform Name in the DB

        """
        return self.get_name_by_id("platform", platform)

    def get_topology_name(self, topology=0):
        """
        Query of the Name of platform ID in the DB

        Args:
            topology (int): The Topology ID

        Returns:
             str : the Topology Name in the DB

        """
        return self.get_name_by_id("az_topology", topology)

    def get_version_builds(self, version=None):
        """
        Query the list of build in specific version

        Args:
            version (str): The version name (e.g. 4.9.0)

        Returns:
             dict : dictionary of (Name: ID), None if not exist

        """
        ver_id = self.get_version_id(version)
        if ver_id is None:
            return None

        results = {}
        query = f"SELECT id, name FROM builds WHERE version = {ver_id} ;"
        if self._run_query(query=query):
            for (build_id, name) in self.cursor:
                results[name] = build_id

        return None if results == {} else results

    def get_build_id(self, version, build):
        """
        Getting the build ID for specific build of version.
        if the build does not exist, return None

        Args:
            version (str): the version name (e.g. 4.9.0)
            build (str): the build name (e.g. GA)

        Returns:
            int : the build ID

        """
        all_builds = self.get_version_builds(version=version)
        if all_builds:
            return all_builds.get(build)
        return None

    def insert_build(self, version, build):
        """
        Insert a new build to the DB and return it's ID

        Args:
            version (str): The version number as string (e.g. 4.9.0)
            build (str): The build number (e.g. 180 / RC1-200 / GA)

        Returns:
             int : the ID of the build in the DB, otherwise None

        """
        # prevent of duplicate records for the same build
        build_id = self.get_build_id(version=version, build=build)
        if build_id is not None:
            return build_id

        # Try to insert the version into the DB, it will not be inserted twice,
        # If the version is exists in the DB it will just return the id of it.
        ver_id = self.insert_single_value(table="versions", value=version)

        query = f"INSERT INTO builds (version, name) VALUES ({ver_id}, '{build}') ;"
        if self._run_query(query=query):
            # Insert the data
            try:
                rec_id = self.cursor.lastrowid
                # Make sure data is committed to the database
                self.cnx.commit()
                return rec_id
            except Exception as err:
                log.error(f"Can not insert {version}-{build} into builds - [{err}]")
                return None

    def get_results(self, version, build, platform, topology, test):
        """
        Getting the results information (es_link, log_file) for all test samples
        for a particular test configuration.

        Args:
            version (str): The version number (e.g. 4.9.0)
            build (str): The build number (e.g. RC5-180)
            platform (str): The platform (e.g.  Bare-Metal)
            topology (str): The topology (e.g. 3-AZ)
            test (str): The test name (e.g. SmallFiles)

        Returns:
             dict : dictionary of all test samples as :
                    {sample: {es_link, log_file},}

        """

        def value_verify(value, msg):
            if value is None:
                log.error(f"{msg} does not exist in the DB!")
                return False
            else:
                return True

        ver_id = self.get_version_id(version=version)
        build_id = self.get_build_id(version=version, build=build)
        platform_id = self.get_platform_id(platform=platform)
        topology_id = self.get_topology_id(topology=topology)
        test_id = self.get_test_id(test=test)
        if not (
            value_verify(ver_id, f"Version : {version}")
            and value_verify(build_id, f"Build : {version}-{build}")
            and value_verify(platform_id, f"Platform : {platform}")
            and value_verify(topology_id, f"Topology : {topology}")
            and value_verify(test_id, f"Test : {test}")
        ):
            return None
        results = {}
        query = (
            f"SELECT sample,es_link,log_file FROM results WHERE version = {ver_id} "
            f"AND build = {build_id} AND platform = {platform_id} AND "
            f"az_topology = {topology_id} AND test_name = {test_id} ;"
        )
        if self._run_query(query=query):
            for (sample, eslink, logfile) in self.cursor:
                log.debug(f"{sample}, {eslink}, {logfile}")
                results[sample] = {
                    "eslink": eslink.rstrip("\r\n"),
                    "log": logfile.rstrip("\r\n"),
                }

        return results

    def get_next_sample(self, version, build, platform, topology, test):
        """
        Getting the the number of the next sample for particular test results.
        if there are no results in the DB, it will return 0

        Args:
            version (str): The version number (e.g. 4.9.0)
            build (str): The build number (e.g. RC5-180)
            platform (str): The platform (e.g.  Bare-Metal)
            topology (str): The topology (e.g. 3-AZ)
            test (str): The test name (e.g. SmallFiles)

        Returns:
             int : the number of the next sample to insert to the DB

        """
        ver_id = self.get_version_id(version=version)
        if ver_id is None:
            return 0

        build_id = self.get_build_id(version=version, build=build)
        if build_id is None:
            return 0

        platform_id = self.get_platform_id(platform)
        if platform_id is None:
            return 0

        topology_id = self.get_topology_id(topology)
        if topology_id is None:
            return 0

        test_id = self.get_test_id(test)
        if test_id is None:
            return 0

        results = []
        query = (
            f"SELECT sample FROM results WHERE version = {ver_id} AND "
            f"build = {build_id} AND platform = {platform_id} AND "
            f"az_topology = {topology_id} AND test_name = {test_id} ;"
        )
        if self._run_query(query=query):
            for sample in self.cursor:
                results.append(sample[0])

        if len(results) == 0:
            return 0
        else:
            return max(results) + 1

    def add_results(self, version, build, platform, topology, test, eslink, logfile):
        """
        Adding results information into the DB.

        Args:
            version (str): The version number (e.g. 4.9.0)
            build (str): The build number (e.g. RC5-180)
            platform (str): The platform (e.g.  Bare-Metal)
            topology (str): The topology (e.g. 3-AZ)
            test (str): The test name (e.g. SmallFiles)
            eslink (str): The elasticsearch link(s) to the results
            logfile (str): The link to the test log file

        Returns:
             bool : True if the operation succeed otherwise False

        """
        ver_id = self.get_version_id(version=version)
        if ver_id is None:
            ver_id = self.insert_single_value(table="versions", value=version)
            if ver_id is None:
                return False

        build_id = self.get_build_id(version=version, build=build)
        if build_id is None:
            build_id = self.insert_build(version=version, build=build)
            if build_id is None:
                return False

        platform_id = self.get_platform_id(platform)
        if platform_id is None:
            platform_id = self.insert_single_value(table="platform", value=platform)
            if platform_id is None:
                return False

        topology_id = self.get_topology_id(topology)
        if topology_id is None:
            topology_id = self.insert_single_value(table="az_topology", value=topology)
            if topology_id is None:
                return False

        test_id = self.get_test_id(test)
        if test_id is None:
            test_id = self.insert_single_value(table="tests", value=test)
            if test_id is None:
                return False

        sample = self.get_next_sample(
            version=version,
            build=build,
            platform=platform,
            topology=topology,
            test=test,
        )

        query = (
            f"INSERT INTO results "
            "(sample, version, build, platform, az_topology, test_name, es_link, log_file) "
            f"VALUES ({sample}, {ver_id}, {build_id}, {platform_id}, {topology_id}, "
            f"{test_id}, '{eslink}', '{logfile}') ;"
        )
        if self._run_query(query=query):
            try:
                # Make sure data is committed to the database
                self.cnx.commit()
                log.info("Test results pushed to the DB!")
                return True
            except Exception as err:
                log.error(f"Can not insert result into the DB - [{err}]")
                return False

    def cleanup(self):
        """
        Cleanup and close the DB connection

        """
        log.info("Closing the DB connection")
        self.cursor.close()
        self.cnx.close()
