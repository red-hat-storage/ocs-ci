# -*- coding: utf8 -*-


class ZoneConfig:
    """
    ZoneConfig is tracking ip addresses of nodes in each cluster zone.
    """

    VALID_ZONES = ("a", "b", "c")

    def __init__(self):
        self._zones = {}

    def add_node(self, zone, node):
        """
        Add a node ip address into a zone.

        Args:
            zone (str): zone identification (one of ``VALID_ZONES``)
            node (str): ip address of a node

        """
        if zone not in self.VALID_ZONES:
            raise ValueError("Invalid zone name: {zone}")
        self._zones.setdefault(zone, set()).add(node)

    def add_nodes(self, zone, nodes):
        """
        Add list of node ip addresses into a zone.

        Args:
            zone (str): zone identification (one of ``VALID_ZONES``)
            nodes (list): list of string representation of node ip addresses
        """
        for node in nodes:
            self.add_node(zone, node)

    def get_nodes(self, zone):
        """
        Return set of node ip addresses in given zone.

        Args:
            zone (str): zone identification (one of ``VALID_ZONES``)

        Returns:
            list: string representation of node ip addresses of given zone

        """
        return self._zones.get(zone)

    def get_env_file(self):
        """
        Generate content of env file for firewall script.

        Returns:
            str: content of firewall environment file with zone configuration

        """
        lines = []
        for zone, node_list in self._zones.items():
            nodes = " ".join(sorted(node_list))
            lines.append(f'ZONE_{zone.upper()}="{nodes}"')
        return "\n".join(lines) + "\n"
