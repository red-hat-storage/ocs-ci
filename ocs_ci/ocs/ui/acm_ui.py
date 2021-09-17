
class ACM(object):
    """
    All ACM UI related utilities lives here
    """
    def __init__(self):
        self.cluster_array = list() 

    def create_cluster(self):
        pass

    def 

class ACMOCPCluster(object):
    """
    A Cluster object which has attributes of a cluster from
    UI perspective

    """
    def __init__(self):
        self.name = None
        self.platform = None
        self.master_node_conf = None
        self.worker_pool_conf = None
        self.network_conf = None

    def get_kubeconfig(self):
        pass

    def check_status(self):
        pass


