from ocs_ci.ocs.node import get_nodes


def main():
    print("Debug Mode Helper")
    nodes = get_nodes()
    for node in nodes:
        print(node.name)
