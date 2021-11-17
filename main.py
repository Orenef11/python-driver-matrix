import os
import logging
import argparse
import subprocess
from typing import List

import run

logging.basicConfig(level=logging.INFO)


def main(python_driver_git, scylla_install_dir, driver_type, tests, versions, protocols, scylla_version):
    results = []
    for version in versions:
        for protocol in protocols:
            logging.info('=== PYTHON DRIVER VERSION {}, PROTOCOL v{} ==='.format(version, protocol))
            results.append(run.Run(python_driver_git, driver_type, scylla_install_dir, version, protocol, tests, scylla_version=scylla_version))

    logging.info('=== PYTHON DRIVER MATRIX RESULTS ===')
    status = 0
    for result in results:
        logging.info(result)
        if result.summary['failure'] > 0 or result.summary['error']:
            logging.info("The 'python-driver-matrix' run failed because there are failures and/or errors")
            status = 1
    quit(status)


def extract_two_latest_repo_tags(repo_directory: str, latest_tags_size: int = 2) -> List[str]:
    commands = f"""
    cd {repo_directory}
    git tag --sort=-creatordate | head -n {latest_tags_size}
    """
    return subprocess.check_output(commands, shell=True).decode().splitlines()


if __name__ == '__main__':
    protocols = ['3', '4']
    parser = argparse.ArgumentParser()
    parser.add_argument('python_driver_git', help='folder with git repository of python-driver')
    parser.add_argument('scylla_install_dir',
                        help='folder with scylla installation, e.g. a checked out git scylla has been built',
                        nargs='?', default='')
    parser.add_argument('--driver-type', help='Type of python-driver ("scylla", "cassandra" or "datastax")',
                        dest='driver_type')
    parser.add_argument('--versions', default="", help='python-driver versions to test')
    parser.add_argument('--tests', default='tests.integration.standard',
                        help='tests to pass to nosetests tool, default=tests.integration.standard')
    parser.add_argument('--protocols', default=protocols,
                        help='cqlsh native protocol, default={}'.format(','.join(protocols)))
    parser.add_argument('--scylla-version', help="relocatable scylla version to use",
                        default=os.environ.get('SCYLLA_VERSION', None))
    arguments = parser.parse_args()

    versions = arguments.versions or extract_two_latest_repo_tags(repo_directory=arguments.python_driver_git)
    if not isinstance(arguments.versions, list):
        versions = arguments.versions.split(',')
    if not isinstance(arguments.protocols, list):
        protocols = arguments.protocols.split(',')
    logging.info('The following python driver versions will test: '.format(', '.join(versions)))
    main(arguments.python_driver_git, arguments.scylla_install_dir, arguments.driver_type, arguments.tests, versions,
         protocols, arguments.scylla_version)
