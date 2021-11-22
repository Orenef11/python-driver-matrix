import logging
import os
import re
import subprocess
from functools import cached_property
from typing import Set

import yaml
import processjunit
import tempfile
from packaging.version import Version, InvalidVersion


class Run:

    def __init__(self, python_driver_git, python_driver_type, scylla_install_dir, tag, protocol, tests,
                 scylla_version=None):
        # In case the tag contains a format version like  '3.24.7.1'
        self._tag = tag.split("-", maxsplit=1)[0]
        self._python_driver_git = python_driver_git
        self._python_driver_type = python_driver_type
        self._scylla_version = scylla_version
        self._scylla_install_dir = scylla_install_dir
        self._tests = tests
        self._protocol = int(protocol)
        self._venv_path = None
        self._version_folder = None
        self._xunit_file = self._get_xunit_file(self._setup_out_dir())
        self._run()

    @cached_property
    def summary(self):
        return self._junit.summary

    def __repr__(self):
        details = dict(version=self._tag, protocol=self._protocol, type=self._python_driver_type)
        details.update(self._junit.summary)
        return '({type}){version}: v{protocol}: testcases: {testcase},' \
            ' failures: {failure}, errors: {error}, skipped: {skipped},' \
            ' ignored_in_analysis: {ignored_in_analysis}'.format(**details)

    @cached_property
    def version_folder(self) -> str:
        if self._version_folder is None:
            self._version_folder = self.__version_folder(self._python_driver_type, self._tag)
        logging.info("Taking patch and ignore files from directory '{}'".format(self._version_folder))
        return self._version_folder

    @staticmethod
    def __version_folder(python_driver_type, target_tag):
        version_pattern = re.compile(r"(\d+.)+\d+$")
        target_version_folder = os.path.join(os.path.dirname(__file__), 'versions', python_driver_type)
        try:
            target_version = Version(target_tag)
        except InvalidVersion:
            target_dir = os.path.join(target_version_folder, target_tag)
            if os.path.exists(target_dir):
                return target_dir
            return os.path.join(target_version_folder, 'master')

        tags_defined = sorted(
            (Version(tag) for tag in os.listdir(target_version_folder) if version_pattern.match(tag)),
            reverse=True
        )
        for tag in tags_defined:
            if tag <= target_version:
                return os.path.join(target_version_folder, str(tag))
        return None

    def _setup_out_dir(self):
        here = os.path.dirname(__file__)
        xunit_dir = os.path.join(here, 'xunit', self._tag)
        if not os.path.exists(xunit_dir):
            os.makedirs(xunit_dir)
        return xunit_dir

    def _get_xunit_file(self, xunit_dir):
        file_path = os.path.join(xunit_dir, 'nosetests.{}.v{}.{}.xml'.format(
            self._python_driver_type, self._protocol, self._tag))
        if os.path.exists(file_path):
            os.unlink(file_path)
        return file_path

    @cached_property
    def ignore_file(self):
        return os.path.join(self.version_folder, 'ignore.yaml')

    def _ignore_tests(self) -> Set[str]:
        if not os.path.exists(self.ignore_file):
            logging.info('Cannot find ignore file for version {}'.format(self._tag))
            return set()

        with open(self.ignore_file) as file:
            content = yaml.safe_load(file)
        ignore_tests = set(content.get("general", []))
        ignore_tests.update(content.get(self._protocol, []))
        if not ignore_tests:
            logging.info("The 'ignore.yaml' for version tag '%s' doesn't contains '%d' element or it's empty"
                         "".format(self._tag, self._protocol))
        return ignore_tests

    def _environment(self):
        result = {}
        result.update(os.environ)
        result['PROTOCOL_VERSION'] = self._protocol
        if self._scylla_version:
            result['SCYLLA_VERSION'] = self._scylla_version
        else:
            result['INSTALL_DIRECTORY'] = self._scylla_install_dir
        return result

    def _apply_patch_files(self) -> bool:
        for file_name in os.listdir(self.version_folder):
            if file_name == "patch" or file_name.endswith(".patch"):
                file_path = os.path.join(self.version_folder, file_name)
                try:
                    subprocess.check_call(f"patch -p1 -i {file_path}", shell=True)
                except Exception as exc:
                    logging.error("Failed to apply patch '{}' to version '{}', with: '{}'".format(
                        file_path, self._tag, str(exc)))
                    return False

    def _get_venv_path(self):
        if self._venv_path is not None:
            return self._venv_path
        self._venv_path = os.path.join(tempfile.gettempdir(), '.venv', self._python_driver_type, self._tag)
        return self._venv_path

    def _create_venv(self):
        subprocess.call(f"python3 -m venv {self._get_venv_path()}".split(), env=self._environment())

    def _activate_venv_cmd(self):
        return f"source {self._get_venv_path()}/bin/activate"

    def _install_python_requirements(self):
        try:
            self._create_venv()
            for requirement_file in ['./requirements.txt', './test-requirements.txt']:
                if not os.path.exists(requirement_file):
                    continue
                subprocess.call(f"{self._activate_venv_cmd()} ; pip install --user --force-reinstall -r {requirement_file}",
                                shell=True,
                                env=self._environment())
            return True
        except Exception as exc:
            logging.error("Failed to install python requirements for version {}, with: {}".format(self._tag, str(exc)))
            return False

    def _checkout_branch(self):
        try:
            subprocess.check_call('git checkout .', shell=True)
            if self._python_driver_type == 'scylla':
                subprocess.check_call('git checkout {}-scylla'.format(self._tag), shell=True)
            else:
                subprocess.check_call('git checkout {}'.format(self._tag), shell=True)
            return True
        except Exception as exc:
            logging.error("Failed to branch for version {}, with: {}".format(self._tag, str(exc)))
            return False

    def _run(self):
        os.chdir(self._python_driver_git)
        if not self._checkout_branch():
            self._publish_fake_result()
            return
        if not self._apply_patch_files():
            self._publish_fake_result()
            return
        if not self._install_python_requirements():
            self._publish_fake_result()
            return
        exclude_str = " ".join(f'--exclude-test {test_name}' for test_name in self._ignore_tests())
        cmd = 'nosetests --with-xunit --xunit-file {} -s {} {}'.format(self._xunit_file, self._tests, exclude_str)
        logging.info(cmd)
        subprocess.call(cmd.split(), env=self._environment())
        self._junit = self._process_output()

    def _process_output(self):
        junit = processjunit.ProcessJUnit(self._xunit_file, self._ignore_tests())
        content = open(self._xunit_file).read()
        open(self._xunit_file, 'w').write(content.replace('classname="', 'classname="version_{}_v{}_'.format(
            self._tag, self._protocol)))
        return junit

    def _publish_fake_result(self):
        self._junit = FakeJunitResults(1, 1, 0, 0)


class FakeJunitResults:
    def __init__(self, testcase, failure, error, skipped):
        self.summary = {
            'testcase': testcase,
            'failure': failure,
            'error': error,
            'skipped': skipped,
            'ignored_in_analysis': 0
        }
