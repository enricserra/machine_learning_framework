import logging
import os

import sys
from traceback import format_exception

from pyCGA.opencgaconfig import ConfigClient
from pyCGA.opencgarestclients import OpenCGAClient


class BerthaOpenCGAException(Exception):
    pass

def get_connector_10(study, url, user, password):
    return BerthaOpenCGA(study,
                         url + "/opencga",
                         user,
                         password,
                         '/genomes',
                         ignore_dirs=True,
                         retry_max_attemps=10,
                         min_retry_seconds=1,
                         max_retry_seconds=2,
                         logger=None,
                         exclude_dirs=None)

class BerthaOpenCGA(object):
    """
    Provides functionality for Bertha-OpenCGA integration. This class takes the
    assumptions made by Bertha about OpenCGA and makes the translation. Example:
    An sample ID in Bertha is an LP Number, while in OpenCGA it's a sample name.
    This class provides a reusable set of Bertha-specific methods to interact
    with OpenCGA.
    The usage pattern is::

        with BerthaOpenCGA(...) as connector:
            do_work

    The connector log in with __enter__ and logout in __exit__
    """

    def __init__(self, study_id, instance_url, username, password, paths_root,
                 ignore_dirs, retry_max_attemps=None,
                 min_retry_seconds=None, max_retry_seconds=None,
                 logger=None, exclude_dirs=None):
        """
        :param study_id: integer study id or study alias
        :param instance_url: example 'localhost:8080/opencga'
        :param username: OpenCGA username (string) for login
        :param password: OpenCGA password (string) for login
        :param paths_root: path root to be stripped from OpenCGA paths, e.g. '/genomes'
        :param ignore_dirs: if True, directories will not be registered
        :param retry_max_attemps: optional: a max number of attempts when retrying.
        No retries by default. If specified, then min_retry_seconds and max_retry_seconds
        must also be supplied.
        :param min_retry_seconds: retry wait time starts at min_retry_seconds and doubles
         after each failure, capped at max_retry_seconds
        :param max_retry_seconds: see min_retry_seconds
        :param logger: optional - if supplied, the connector will log messages in this logger
        :param exclude_dirs: a list of strings - any file which path starts with one of the
        strings in this list will not be linked to catalog.
        """
        self.study_id = study_id
        self.instance_url = instance_url
        self.username = username
        self.password = password
        self.paths_root = paths_root
        self.ignore_dirs = ignore_dirs

        if retry_max_attemps:
            assert min_retry_seconds and max_retry_seconds, \
                "If retry_max_attemps is supplied, min_retry_seconds and max_retry_seconds must be supplied"
        self.retry_max_attempts = retry_max_attemps
        self.min_retry_seconds = min_retry_seconds
        self.max_retry_seconds = max_retry_seconds

        self.logger = logger
        self.exclude_dirs = exclude_dirs or []

        # a cache mapping sample lp numbers to OpenCGA sample IDs to reduce number of requests
        self.sample_id_cache = {}
        config = ConfigClient.get_basic_config_dict(self.instance_url)
        config['retry'] = dict(
            max_attempts=self.retry_max_attempts,
            min_retry_seconds=self.min_retry_seconds,
            max_retry_seconds=self.max_retry_seconds
        )
        self.pycga = OpenCGAClient(config, self.username, self.password, on_retry=self._on_pycga_retry)
        self._log("Connected and logged in")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pycga.logout()
        self.pycga = None

    # noinspection PyUnusedLocal
    def _on_pycga_retry(self, pycga, exc_type, exc_val, exc_tb, call):
        exc_str = ''.join(format_exception(exc_type, exc_val, exc_tb))
        msg = """pyCGA call with
                 {call}
                 failed with:
                 {failure}
                 Retrying...""".format(call=call, failure=exc_str)
        self._log(msg, logging.ERROR)

    def _log(self, msg, level=logging.INFO, *args, **kwargs):
        if self.logger:
            self.logger.log(level, msg, *args, **kwargs)
        return None

    def _get_file_id(self, path):
        """
        Gets the id of a file given its path
        :param path: string - absolute or OpenCGA path
        :return: an integer - the internal OpenCGA file ID. Raises BerthaOpenCGAException if not
        found or not unique.
        """
        results = self.pycga.files.search(self.study_id, path=self.to_catalog_path(path),
                                          include='id').get()
        if len(results) == 1:
            return results[0]['id']  # Ok
        elif len(results) > 1:
            raise BerthaOpenCGAException("More than one file found for path {} in OpenCGA. Ids: {}"
                                         .format(path, ','.join([str(result['id']) for result in results])))
        else:
            raise BerthaOpenCGAException("File {} not found in OpenCGA".format(path))

    def get_all_samples(self, limit=1000000):
        samples = self.pycga.samples.search(study=self.study_id, name="~LP", limit=limit)
        return samples[0]

    def to_catalog_path(self, path):
        """
        If path starts with self.paths_root, strips that root and any leading '/' that
        might be left. Otherwise, no changes.
        :return: the transformed path. Examples, assuming self.paths_root == '/genomes':
            /genomes/dir/file.txt --> 'dir/file.txt'
            genomes/dir/file.txt --> 'genomes/dir/file.txt'
            /dir/file.txt --> '/dir/file.txt'
        """
        if path.startswith(self.paths_root):
            return path[len(self.paths_root):].lstrip('/')
            # left-strip paths_root (don't use lstrip for that!) and then any remaining leading '/'
        else:
            return path

    @staticmethod
    def create_tracking_entry(process_name, process_version,
                              process_time, **kwargs):
        entry = {
            'name': process_name,
            'version': process_version,
            'processDate': "{:%Y%m%d%H%M%S}".format(process_time)  # this is the opencga datetime format
        }
        entry.update(kwargs)
        return entry

    @staticmethod
    def contains_tracking_item(processes, item, ignore_lookup=None):
        if ignore_lookup is None:
            ignore_lookup = set()
        else:
            ignore_lookup = set(ignore_lookup)  # ensure it's a set
        ignore_lookup.add('processDate')

        # compose a json path expression, e.g. '$[?name=="Bertha" & version=="1.3.2" & runId==1]'
        filter_parts = ["{}=={}".format(key, jsonpath.expr_str(value))
                        for key, value in item.iteritems()
                        if key not in ignore_lookup]
        pattern = "$[?{filter}]".format(filter=" & ".join(filter_parts))
        return bool(jsonpath.match(pattern, processes))

    def sample_search(self, lp_number, **options):
        """
        Get a sample from the current study in openCGA given its LP number. If not found, then returns None. If
        there is more than 1 sample with that LP number, then raises an BerthaOpenCGAException
        :param lp_number: LP number of the sample, which is store in the 'name' field in OpenCGA. A string.
        :return: A dictionary representing a sample, as returned by OpenCGA, or None if not found.
        """
        results = self.pycga.samples.search(study=self.study_id, name=lp_number, **options).get()
        if len(results) == 1:
            sample = results[0]
            self.sample_id_cache[lp_number] = sample['id']
            return sample  # Ok
        elif len(results) > 1:
            raise BerthaOpenCGAException("More than one sample found for LP Number {}. OpenCGA sample IDs: {}"
                                         .format(lp_number, ','.join([str(result['id']) for result in results])))
        else:
            return None  # Not found

    def sample_get(self, lp_number, **options):
        """Gets a sample by LP number or raises a BerthaOpenCGAException if not found
        :param lp_number: string, sample lp number, matched against "name" field
        :param options: additional query options, e.g. include=stats
        :return: A sample dictionary as returned by OpenCGA, or BerthaOpenCGAException
        if not found
        """
        sample = self.sample_search(lp_number, **options)
        if sample is None:
            raise BerthaOpenCGAException("Sample {} not found in OpenCGA".format(lp_number))
        return sample

    def sample_id_get(self, lp_number):
        """If a sample is in the sample cache, returns its id from cache,
        otherwise, looks it up in OpenCGA. If not found, raises a
        BerthaOpenCGAException
        :param lp_number: string, sample lp number, matched against "name" field
        :return: integer: OpenCGA sample ID, or BerthaOpenCGAException if not found.
        """
        sample_id = self.sample_id_cache.get(lp_number)
        if sample_id is not None:
            return sample_id
        else:
            sample = self.sample_search(lp_number)
            if not sample:
                raise BerthaOpenCGAException("Sample {} not found in OpenCGA".format(lp_number))
            return sample['id']


    def file_get(self, file_id, **options):
        """Gets a file identified by its OpenCGA file ID.
        :return: File dictionary as returned by OpenCGA. If not found, raises BerthaOpenCGAException
        """
        catalog_file = self.pycga.files.search(study=self.study_id, id=file_id, **options).get()
        if not catalog_file:
            raise BerthaOpenCGAException("File {} not found in OpenCGA".format(file_id))
        else:
            return catalog_file[0]

    def file_get_all_associated_with_sample(self, sample):
        files = self.pycga.files.search(study=self.study_id, sampleIds=sample)
        return files[0]

def get_default_opencga_connector(study_id, logger=None, exclude_dirs=None):
    """
    Creates a BerthaOpenCGA with default bertha configuration values (url, username
    password, genomes root from environment variables)
    :param study_id: Study ID to be used in the connector
    :param logger: optional, if supplied then the connector will log on this logger,
    otherwise the default module logger will be used
    :param exclude_dirs: list of directory paths (strings) to be excluded from registration
    :return:
    """
    if is_opencga_bypassed():
        raise BerthaOpenCGAException("Can't use OpenCGA connector because OpenCGA integration is bypassed")

    if logger is None:
        logger = setup_logging(__name__)
    if exclude_dirs is None:
        exclude_dirs = [get_resources().base_dir]
    return BerthaOpenCGA(study_id,
                         get_default_opencga_url(),
                         get_default_opencga_user(),
                         get_default_opencga_password(),
                         get_default_root_path(),
                         ignore_dirs=True,
                         retry_max_attemps=sys.maxint,
                         min_retry_seconds=INITIAL_CATALOG_RETRY_SECONDS,
                         max_retry_seconds=MAX_CATALOG_RETRY_SECONDS,
                         logger=logger,
                         exclude_dirs=exclude_dirs)
