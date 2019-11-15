"""
CSV related functionalities
"""
from ocs_ci.ocs.ocp import OCP


class CSV(OCP):
    """
    This class represent ClusterServiceVersion (CSV) and contains all related
    methods we need to do with CSV.
    """

    _has_phase = True

    def __init__(self, resource_name="", *args, **kwargs):
        """
        Initializer function for CSV class

        Args:
            resource_name (str): Name of CSV

        """
        super(CSV, self).__init__(
            resource_name=resource_name, kind='csv', *args, **kwargs
        )


def get_csvs_start_with_prefix(csv_prefix, namespace):
    """
    Get CSVs start with prefix

    Args:
        csv_prefix (str): prefix from name
        namespace (str): namespace of CSV

    Returns:
        list: found CSVs

    """

    csvs = CSV(namespace=namespace)
    csv_list = csvs.get()['items']
    return [csv for csv in csv_list if csv_prefix in csv['metadata']['name']]
