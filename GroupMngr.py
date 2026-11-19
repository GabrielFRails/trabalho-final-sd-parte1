"""XML-RPC service responsible for peer membership."""

from xmlrpc.server import SimpleXMLRPCServer

from constMP import GROUPMNGR_BIND_ADDR, GROUPMNGR_RPC_PORT


class GroupManager:
    def __init__(self):
        self.membership = []
        self.stop_requested = False

    def register(self, host, udp_port, rpc_port):
        peer = {
            "host": str(host),
            "udp_port": int(udp_port),
            "rpc_port": int(rpc_port),
        }
        if peer not in self.membership:
            self.membership.append(peer)
        print("Registered peer:", peer)
        return True

    def list_peers(self):
        peers = [peer.copy() for peer in self.membership]
        print("List of peers sent:", peers)
        return peers

    def unregister(self, host, udp_port, rpc_port):
        peer = {
            "host": str(host),
            "udp_port": int(udp_port),
            "rpc_port": int(rpc_port),
        }
        if peer in self.membership:
            self.membership.remove(peer)
            print("Unregistered peer:", peer)
            return True
        return False

    def stop(self):
        print("Stopping Group Manager.")
        self.stop_requested = True
        return True


def main():
    manager = GroupManager()
    with SimpleXMLRPCServer(
        (GROUPMNGR_BIND_ADDR, GROUPMNGR_RPC_PORT),
        allow_none=True,
        logRequests=False,
    ) as server:
        server.register_introspection_functions()
        server.register_instance(manager)
        server.timeout = 0.5

        print(
            "Group Manager RPC listening on "
            f"{GROUPMNGR_BIND_ADDR}:{GROUPMNGR_RPC_PORT}"
        )
        while not manager.stop_requested:
            server.handle_request()


if __name__ == "__main__":
    main()
