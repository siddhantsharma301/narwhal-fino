# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from math import ceil
from os.path import basename, splitext
import pandas as pd
from time import sleep

from benchmark.commands import CommandMaker, WeldCommandMaker
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, ConfigError, WeldLocalCommittee
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class LocalBench:
    BASE_PORT = 3000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT, self.workers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Run the clients (they will wait for the nodes to be ready).
            workers_addresses = committee.workers_addresses(self.faults)
            rate_share = ceil(rate / committee.workers())
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    cmd = CommandMaker.run_client(
                        address,
                        self.tx_size,
                        rate_share,
                        [x for y in workers_addresses for _, x in y]
                    )
                    log_file = PathMaker.client_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Run the primaries (except the faulty ones).
            for i, address in enumerate(committee.primary_addresses(self.faults)):
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    debug=debug
                )
                log_file = PathMaker.primary_log_file(i)
                self._background_run(cmd, log_file)

            # Run the workers (except the faulty ones).
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        debug=debug
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process(PathMaker.logs_path(), faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)
        
    def run_several(self, debug=False, runs=5):
        count, runs = 0, runs
        c_tps, c_bps, c_lat = [], [], []
        e2e_tps, e2e_bps, e2e_lat = [], [], []
        while count < runs:
            Print.info(f"RUN {count}")
            try:
                parsed = self.run(debug=debug)
                consensus_tps, consensus_bps, _ = parsed._consensus_throughput()
                if consensus_tps > 0:
                    count += 1
                else:
                    Print.info("Run failed, trying again due to no tps")
                    continue
                consensus_latency = parsed._consensus_latency() * 1_000
                end_to_end_tps, end_to_end_bps, _ = parsed._end_to_end_throughput()
                end_to_end_latency = parsed._end_to_end_latency() * 1_000
                
                c_tps.append(consensus_tps)
                c_bps.append(consensus_bps)
                c_lat.append(consensus_latency)
                e2e_tps.append(end_to_end_tps)
                e2e_bps.append(end_to_end_bps)
                e2e_lat.append(end_to_end_latency)
            except Exception as e:
                raise e
        
        df = pd.DataFrame({
            "Consensus TPS": c_tps, 
            "Consensus BPS": c_bps, 
            "Consensus Latency": c_lat,
            "End-to-end TPS": e2e_tps, 
            "End-to-end BPS": e2e_bps, 
            "End-to-end latency": e2e_lat
        })
        Print.info(f"{df.describe()}")
        return parsed
    
class WeldLocalBench(LocalBench):
    def __init__(self, bench_parameters_dict, node_parameters_dict):
        super(LocalBench, self).__init__(bench_parameters_dict, node_parameters_dict)

    def _kill_nodes(self):
        try:
            cmd = WeldCommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f'{WeldCommandMaker.clean_logs()} ; {WeldCommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = WeldCommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = WeldCommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = WeldCommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = WeldLocalCommittee(names, self.BASE_PORT, self.workers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Run the clients (they will wait for the nodes to be ready).
            workers_addresses = committee.workers_addresses(self.faults)

            for i, address in enumerate(committee.app_addresses(self.faults)):
                cmd = WeldCommandMaker.run_app(address)
                log_file = PathMaker.app_log_file(i)
                # Each one of these starts a new tmux session
                self._background_run(cmd, log_file)

            sleep(1)

            rate_share = ceil(rate / committee.workers())
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    cmd = WeldCommandMaker.run_client(
                        address,
                        self.tx_size,
                        rate_share,
                        [x for y in workers_addresses for _, x in y]
                    )
                    log_file = PathMaker.client_log_file(i, id)
                    self._background_run(cmd, log_file)


            # Run the primaries (except the faulty ones).
            for i, address in enumerate(committee.primary_addresses(self.faults)):
                cmd = WeldCommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    app_api = committee.app_addresses(self.faults)[i],
                    abci_api = committee.rpc_addresses(self.faults)[i],
                    debug=debug
                )
                log_file = PathMaker.primary_log_file(i)
                self._background_run(cmd, log_file)


            # Run the workers (except the faulty ones).
            for i, addresses in enumerate(workers_addresses):
                for (id, address) in addresses:
                    cmd = WeldCommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        debug=debug
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    self._background_run(cmd, log_file)


            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process(PathMaker.logs_path(), faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            print(e)
            raise BenchError('Failed to run benchmark', e)
