// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, SecretKey};
use primary::Header;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::collections::{BTreeSet, VecDeque};
use tokio::sync::mpsc::channel;

// Fixture
fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture
pub fn mock_committee() -> Committee {
    Committee {
        authorities: keys()
            .iter()
            .map(|(id, _)| {
                (
                    *id,
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: "0.0.0.0:0".parse().unwrap(),
                            worker_to_primary: "0.0.0.0:0".parse().unwrap(),
                        },
                        workers: HashMap::default(),
                    },
                )
            })
            .collect(),
    }
}

// Fixture
fn mock_certificate(
    origin: PublicKey,
    round: Round,
    decision: Decision,
    parents: BTreeSet<Digest>,
) -> (Digest, Certificate) {
    let certificate = Certificate {
        header: Header {
            author: origin,
            round,
            parents,
            decision,
            ..Header::default()
        },
        ..Certificate::default()
    };
    (certificate.digest(), certificate)
}

// Creates one certificate per authority starting and finishing at the specified rounds (inclusive).
// Outputs a VecDeque of certificates (the certificate with higher round is on the front) and a set
// of digests to be used as parents for the certificates of the next round.
fn make_certificates(
    start: Round,
    stop: Round,
    initial_parents: &BTreeSet<Digest>,
    keys: &[PublicKey],
) -> (VecDeque<Certificate>, BTreeSet<Digest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in start..=stop {
        next_parents.clear();
        let v = round / 2;
        for name in 0..keys.len() {
            let decision = 
                if round % 2 == 1 {
                    Decision::Ok(v)
                } else {
                    // Leave as 0 because consensus leader fn defaults to 0 for test cfg
                    if name == 0 {
                        Decision::Propose(v) 
                    } else { 
                        Decision::Empty 
                    }
                };
            let (digest, certificate) = 
                    mock_certificate(keys[name], round, decision, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents.clone();
    }
    (certificates, next_parents)
}

// Run for 2 dag rounds in ideal conditions (all nodes reference all other nodes). We should commit
// the leader of round 2.
#[tokio::test]
async fn commit_one() {
    // Make certificates for rounds 1 and 2.
    let keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    let committee = mock_committee();
    let genesis = Certificate::genesis(&committee)
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, _) = make_certificates(1, 3, &genesis, &keys);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    let mut counter = 0;
    let commit_trigger = 10;
    while let Some(certificate) = certificates.pop_front() {
        if counter == commit_trigger {
            break;
        }
        tx_waiter.send(certificate).await.unwrap();
        counter += 1;
    }

    // Ensure the first 4 ordered certificates are from round 1 (they are the parents of the committed
    // leader); then the leader's certificate should be committed.
    for i in 0..=4 {
        let certificate = rx_output.recv().await.unwrap();
        if i < 4 {
            assert_eq!(certificate.round(), 1);
            assert_eq!(certificate.view().unwrap(), 0);
        } else {
            assert_eq!(certificate.round(), 2);
            assert_eq!(certificate.view().unwrap(), 1);
        }
    }
}

// Run for 8 dag rounds with one dead node node (that is not a leader). We should commit the leaders of
// rounds 2, 4, 6, and 8.
#[tokio::test]
async fn dead_node() {
    // Make the certificates.
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort(); // Ensure we don't remove one of the leaders.
    let _ = keys.pop().unwrap();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let (mut certificates, _) = make_certificates(1, 9, &genesis, &keys);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }
    });

    // We should commit 4 leaders (rounds 2, 4, 6, and 8).
    for i in 1..=21 {
        let certificate = rx_output.recv().await.unwrap();
        let expected = ((i - 1) / keys.len() as u64) + 1;
        assert_eq!(certificate.round(), expected);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 8);
}

// Run for 5 dag rounds. The leaders of round 2 does not have enough support, but the leader of
// round 4 does. The leader of rounds 2 and 4 should thus be committed.
#[tokio::test]
async fn not_enough_support() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Round 1: Fully connected graph.
    let nodes: Vec<_> = keys.iter().cloned().take(3).collect();
    // let nodes = keys.clone();
    let (out, parents) = make_certificates(1, 1, &genesis, &nodes);
    certificates.extend(out);

    // Round 2: Fully connect graph. But remember the digest of the leader. Note that this
    // round is the only one with 4 certificates.
    let nodes = keys.clone();
    let (out, mut parents) = make_certificates(2, 2, &parents, &nodes);
    let leader_2_digest = out[0].digest();
    parents.remove(&leader_2_digest);
    certificates.extend(out);

    // Round 3: Only node 0 links to the leader of round 2.
    let mut next_parents = BTreeSet::new();

    let name = &keys[1];
    let (digest, certificate) = mock_certificate(*name, 3, Decision::Empty, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    let name = &keys[2];
    let (digest, certificate) = mock_certificate(*name, 3, Decision::Empty, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    let name = &keys[0];
    parents.insert(leader_2_digest);
    let (digest, certificate) = mock_certificate(*name, 3, Decision::Ok(3/2), parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    parents = next_parents.clone();

    // Rounds 4: Fully connected graph.
    let nodes: Vec<_> = keys.iter().cloned().take(3).collect();
    let (out, parents) = make_certificates(4, 4, &parents, &nodes);
    certificates.extend(out);

    // Round 5: Send f+1 certificates to trigger the commit of leader 4.
    let (_, certificate) = mock_certificate(keys[0], 5, Decision::Ok(5/2), parents.clone());
    certificates.push_back(certificate);
    let (_, certificate) = mock_certificate(keys[1], 5, Decision::Ok(5/2), parents);
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });
    
    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    // We should commit 2 leaders (rounds 2 and 4).
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 1);
    }
    for _ in 1..=4 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 2);
    }
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 3);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 4);
}

// Run for 7 dag rounds. Node 0 (the leader of round 2) is missing for rounds 1 and 2,
// and reappears from round 3.
#[tokio::test]
async fn missing_leader() {
    let mut keys: Vec<_> = keys().into_iter().map(|(x, _)| x).collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Remove the leader for rounds 1 and 2.
    let (mut out, parents) = make_certificates(1, 2, &genesis, &keys);
    let mut new_out: VecDeque<Certificate> = VecDeque::new();
    out.pop_front();
    for _ in 0..3 {
        new_out.push_back(out.pop_front().unwrap());
    }
    out.pop_front();
    for _ in 0..3 {
        new_out.push_back(out.pop_front().unwrap());
    }
    certificates.extend(new_out);

    // Add back the leader for rounds 3 and 4.
    let (out, parents) = make_certificates(3, 4, &parents, &keys);
    certificates.extend(out);

    // Add f+1 certificates of round 5 to commit the leader of round 4.
    let (_, certificate) = mock_certificate(keys[0], 5, Decision::Ok(5/2), parents.clone());
    certificates.push_back(certificate);
    let (_, certificate) = mock_certificate(keys[1], 5, Decision::Ok(5/2), parents.clone());
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(
        mock_committee(),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. We should only commit upon receiving the last
    // certificate, so calls below should not block the task.
    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    // Ensure the commit sequence is as expected.
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 1);
    }
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 2);
    }
    for _ in 1..=4 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round(), 3);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round(), 4);
}