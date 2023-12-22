// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Decision, Round};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;
type LogicalDag = HashMap<Round, HashMap<PublicKey, Vec<(Digest, Certificate)>>>;

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed round.
    last_committed_round: Round,
    /// The last committed view.
    last_committed_view: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
    /// Keeps the latest committed certificates ordered by view for every authorithy. Anything older 
    /// must be regularly cleaned up through the function `update`.
    /// TODO: Fix update to clear logical dag state 
    logical_dag: LogicalDag,
    seen_leader: HashMap<Round, bool>,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();
        let mut init_logical_dag: LogicalDag = HashMap::new();
        for (r, v1) in [(0, genesis.clone())].iter().cloned().collect::<HashMap<_, _>>() {
            for (pk, dc) in v1 {
                init_logical_dag
                    .entry(r)
                    .or_insert_with(|| HashMap::new())
                    .entry(pk)
                    .or_insert_with(|| Vec::new())
                    .push(dc);
            }
        }

        Self {
            last_committed_round: 0,
            last_committed_view: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            dag: [(0, genesis.clone())].iter().cloned().collect(),
            logical_dag: init_logical_dag,
            seen_leader: HashMap::from([(0, false)]),
        }
    }

    // TODO: Fix this function to work for newly committed views
    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r: &mut u64| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        for (name, round) in &self.last_committed {
            self.dag.retain(|r: &u64, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

pub struct Consensus {
    /// The committee information.
    committee: Committee,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_primary: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<Certificate>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                gc_depth,
                rx_primary,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_primary.recv().await {
            debug!("Processing {:?}", certificate);
            let round = certificate.round();
            let wrapped_view = certificate.view();
            let view = if wrapped_view.is_some() {
                wrapped_view.unwrap()
            } else {
                std::u64::MAX
            };

            // Add the new certificate to the local storage.
            state.dag.entry(round).or_insert_with(HashMap::new).insert(
                certificate.origin(),
                (certificate.digest(), certificate.clone()),
            );
            if wrapped_view.is_some() {
                state
                    .logical_dag
                    .entry(view)
                    .or_insert_with(HashMap::new)
                    .entry(certificate.origin())
                    .or_insert_with(|| Vec::new())
                    .push((certificate.digest(), certificate.clone()));
            }

            // Already considered committing this view above if valid
            if view <= state.last_committed_view {
                continue;
            }
            let (_leader_digest, leader) = match self.leader(view, &state.logical_dag) {
                Some(x) => x,
                None => continue,
            };
            if state.seen_leader.contains_key(&view) {
                let seen = *state.seen_leader.get(&view).unwrap();
                if !seen {
                    *state.seen_leader.get_mut(&view).unwrap() =
                        leader.origin() == certificate.origin();
                }
            }

            // Get the certificate's digest of the leader. If we already ordered this leader, there is nothing to do.
            let leader_round = leader.round() - 1;
            if leader_round <= state.last_committed_round || leader.round() < 2 {
                continue;
            }

            // Try to see if leader of this view committed
            let ok_stake: Stake = state.logical_dag
                .get(&view)
                .expect("We should have the whole history by now")
                .values()
                .map(|vals| 
                    vals
                        .iter()
                        .filter(|(_, x)| {
                            match x.view() {
                                Some(v) => x.decision() == Decision::Ok(v),
                                _ => false,
                            }
                        })
                        .map(|(_, x)| self.committee.stake(&x.origin())).sum::<Stake>())
                .sum();
            
            if ok_stake < self.committee.validity_threshold() {
                debug!("Leader {:?} does not have enough support", leader);
                continue;
            }
            state.last_committed_view = view;
            state.last_committed_round = leader_round;

            // Get an ordered list of past leaders that are linked to the current leader.
            debug!("Leader {:?} has enough support in round {:?} and decision {:?}", leader, leader.round(), leader.decision());
            let mut sequence = Vec::new();
            for leader in self.order_leaders(leader, &state).iter().rev() {
                // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
                for x in self.order_dag(leader, &state) {
                    // Update and clean up internal state.
                    state.update(&x, self.gc_depth);

                    // Add the certificate to the sequence.
                    sequence.push(x);
                }
            }

            // Log the latest committed round of every authority (for debug).
            if log_enabled!(log::Level::Debug) {
                for (name, round) in &state.last_committed {
                    debug!("Latest commit of {}: Round {}", name, round);
                }
            }

            // Output the sequence in the right order.
            for certificate in sequence {
                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", certificate.header, digest);
                }

                self.tx_primary
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send certificate to primary");

                if let Err(e) = self.tx_output.send(certificate).await {
                    warn!("Failed to output certificate: {}", e);
                }
            }
        }
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(&self, round: Round, dag: &'a LogicalDag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        #[cfg(test)]
        let seed = 0;
        #[cfg(not(test))]
        let seed = round;

        // Elect the leader.
        let leader = self.committee.leader(seed as usize);

        // Return its certificate and the certificate's digest. (any certificate is fine, since they 
        // come from the same leader and view)
        let has_leader = dag.get(&round).map(|x| x.get(&leader)).flatten();
        match has_leader {
            Some(_) => { 
                let leader = has_leader.unwrap().get(0);
                match leader {
                    Some(_) => {
                        let view = leader.unwrap().1.view().unwrap();
                        if leader.unwrap().1.decision() == Decision::Propose(view) {
                            leader
                        } else {
                            None
                        }
                    },
                    _ => None,
                }
                // if leader.is_some() && leader.unwrap().1.decision() == Decision::Propose {
                //     return leader;
                // } else {
                //     return None;
                // }
            },
            None => None,
        }
    }

    /// Order the past leaders that we didn't already commit.
    fn order_leaders(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        for v in (state.last_committed_view + 1..leader.view().unwrap()).rev() {
            let (_, prev_leader) = match self.leader(v, &state.logical_dag) {
                Some(x) => x,
                None => continue,
            };
            if self.linked(leader, prev_leader, &state.logical_dag) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }
        to_commit
    }

    /// Checks if there is a path between two leaders.
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &LogicalDag) -> bool {
        let mut parents = vec![leader];
        for v in (prev_leader.view().unwrap()..leader.view().unwrap()).rev() {
            parents = dag
                .get(&v)
                .expect("We should have the whole history by now")
                .values()
                .filter(|vals| 
                    vals
                        .iter()
                        .filter(|(_digest, _)| parents.iter().any(|x| *x == prev_leader))
                        .collect::<Vec<_>>().len() > 0
                )
                .flat_map(|vals| vals.iter().map(|(_, certificate)| certificate))
                .collect::<Vec<_>>();
            if parents.len() >= 1 {
                return true;
            }
        }
        false
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}