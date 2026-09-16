//! Message passing, as seen by a node.
//!
//! The interface is deliberately impoverished: fire-and-forget bytes to a peer.
//! There is no delivery confirmation, no ordering guarantee, and no way to ask
//! whether a peer is reachable. Anything a node believes about its peers has to
//! be inferred from messages it actually received, which is the discipline that
//! makes a protocol survive a real network.

use simcore::NodeId;

pub trait Net {
    /// Who this node is.
    fn me(&self) -> NodeId;

    /// Total number of nodes in the cluster. Membership is static here;
    /// reconfiguration is out of scope (see the README).
    fn cluster_size(&self) -> usize;

    /// Send bytes to a peer. May be dropped, duplicated, delayed, reordered,
    /// or corrupted. Sending to oneself is a no-op that would otherwise hide
    /// bugs in quorum counting.
    fn send(&mut self, to: NodeId, payload: &[u8]);
}

/// Every node except me, in a fixed order.
pub fn peers(net: &dyn Net) -> Vec<NodeId> {
    let me = net.me();
    (0..net.cluster_size() as u32)
        .map(NodeId)
        .filter(|n| *n != me)
        .collect()
}

/// Smallest number of nodes that constitutes a majority.
pub fn quorum(cluster_size: usize) -> usize {
    cluster_size / 2 + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeNet {
        me: NodeId,
        n: usize,
        sent: Vec<(NodeId, Vec<u8>)>,
    }

    impl Net for FakeNet {
        fn me(&self) -> NodeId {
            self.me
        }
        fn cluster_size(&self) -> usize {
            self.n
        }
        fn send(&mut self, to: NodeId, payload: &[u8]) {
            if to != self.me {
                self.sent.push((to, payload.to_vec()));
            }
        }
    }

    #[test]
    fn peers_excludes_self_and_is_ordered() {
        let net = FakeNet {
            me: NodeId(2),
            n: 5,
            sent: vec![],
        };
        assert_eq!(
            peers(&net),
            vec![NodeId(0), NodeId(1), NodeId(3), NodeId(4)]
        );
    }

    #[test]
    fn quorum_is_a_strict_majority() {
        assert_eq!(quorum(1), 1);
        assert_eq!(quorum(2), 2);
        assert_eq!(quorum(3), 2);
        assert_eq!(quorum(4), 3);
        assert_eq!(quorum(5), 3);
        // Any two quorums of the same cluster must intersect.
        for n in 1..40 {
            assert!(2 * quorum(n) > n, "quorums of {n} would not intersect");
        }
    }

    #[test]
    fn sending_to_self_is_dropped() {
        let mut net = FakeNet {
            me: NodeId(0),
            n: 3,
            sent: vec![],
        };
        net.send(NodeId(0), b"loopback");
        net.send(NodeId(1), b"real");
        assert_eq!(net.sent.len(), 1);
    }
}
