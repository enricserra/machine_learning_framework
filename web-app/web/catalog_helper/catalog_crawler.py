import re


class CatalogCrawler:
    def __init__(self, connector, summary_dir, logger, samples_file, sample_limit=100000):
        self.connector = connector
        self.aggregated_paths = {}
        self.sample_regex = r'LP\d{7}-DNA_[A-H](0[1-9]|1[0-2])'
        self.normal_sample_regex = r'NormalLP\d{7}-DNA_[A-H](0[1-9]|1[0-2])'
        self.cancer_sample_regex = r'CancerLP\d{7}-DNA_[A-H](0[1-9]|1[0-2])'
        self.delivery_regex = r'^((?:RAREP|RARET|CANCP|CANCT)[0-9]{5}|(?:HX|CF|CH|OX|VD|BE|ED)[0-9]{8}|V_V[0-9]{11}|[0-9]{10})$$'
        self.sample_sub = "SAMPLE_ID"
        self.cancer_sample_sub = "NORMAL_SAMPLE_ID"
        self.normal_sample_sub = "CANCER_SAMPLE_ID"
        self.delivery_sub = "DELIVERY_ID"
        self.genotyping_array_idat_red_regex = r'[0-9]{12}[_]{1}[A-z a-z]{1}[0-9]{2}[A-z a-z]{1}[0-9]{2}[_]RED'
        self.genotyping_array_idat_green_regex = r'[0-9]{12}[_]{1}[A-z a-z]{1}[0-9]{2}[A-z a-z]{1}[0-9]{2}[_]GRN'
        self.genotyping_array_gtc_regex = r'[0-9]{12}[_]{1}[A-z a-z]{1}[0-9]{2}[A-z a-z]{1}[0-9]{2}'
        self.genotyping_array_idat_red_sub = "GENOTYPING_IDAT_RED"
        self.genotyping_array_idat_green_sub = "GENOTYPING_IDAT_GREEN"
        self.genotyping_array_gtc_sub = "GENOTYPING_GTC"
        self.summary_dir = summary_dir
        self.open_files = {}
        self.files_2_code = {}
        self.last_file = 1
        self.blacklisted_attributes = ['path', 'uri']
        self.blacklisted_file_endings = ['log']
        self.sample_limit = sample_limit
        self.logger = logger
        self.already_processed = self.load_previous_samples(samples_file)
        self.samples_file = open(samples_file, 'a')

    def load_previous_samples(self, samples_file):
        fh = open(samples_file, 'r')
        result = [line.rstrip('\n') for line in fh]
        fh.close()
        return result

    def summarize(self):
        self.logger.info("Getting samples")
        self.samples = self.connector.get_all_samples(self.sample_limit)
        self.logger.info("Got {} samples, proceeding...".format(len(self.samples)))
        print("Got {} samples, proceeding...".format(len(self.samples)))

        for sample in self.samples:
            if sample['name'] not in self.already_processed:
                self.logger.info("Getting files for sample {}".format(sample['name']))
                print("Getting files for sample {}".format(sample['name']))

                files = self.connector.file_get_all_associated_with_sample(sample['name'])
                self.logger.info("Got {} Files for sample {}".format(len(files), sample['name']))
                self.process_files(files)
                self.already_processed.append(sample['name'])
                self.samples_file.write(sample['name'] + '\n')
            else:
                self.logger.info("Sample {} was already processed, ignoring it".format(sample))

    def process_files(self, files):
        for file in files:
            if self.transform_path(file['path']) not in self.aggregated_paths:
                self.aggregated_paths[self.transform_path(file['path'])] = {}
            self.add_file_to_aggregated(file)

    def should_blacklist_file(self, file):
        for forbiden_ending in self.blacklisted_file_endings:
            if file['path'].endswith(forbiden_ending):
                return True
        return False

    def add_file_to_aggregated(self, file):
        if not self.should_blacklist_file(file):
            for attr in file:
                path = self.join_attr_path(":", self.transform_path(file['path']), attr)
                self.register_attr(attr, file[attr], path)

    @staticmethod
    def join_attr_path(sep, path, attr):
        return sep.join([path, attr])

    def register_attr(self, attr, attr_value, path):
        path = self.transform_path(path)
        if isinstance(attr_value, dict):
            for new_attr in attr_value:
                new_path = self.join_attr_path(":", path, new_attr)
                self.register_attr(new_attr, attr_value[new_attr], new_path)
        elif isinstance(attr_value, list):
            for subval in attr_value:
                self.register_attr(attr, subval, path)
        else:
            if attr not in self.blacklisted_attributes:
                self.send_to_file(path, attr_value)

    def get_physical_file_path(self, path):
        if path not in self.files_2_code:
            self.files_2_code[path] = self.last_file
            self.last_file = self.last_file + 1
            self.open_files[self.files_2_code[path]] = open(self.summary_dir + "/"
                                                            + str(self.files_2_code[path]), 'w')
            self.open_files[self.files_2_code[path]].write(str(path) + "\n\n")
            self.open_files[self.files_2_code[path]].close()
        return self.summary_dir + "/" + str(self.files_2_code[path])

    def send_to_file(self, path, attr_value):
        fh = open(self.get_physical_file_path(str(path)), 'a')
        fh.write(str(attr_value) + "\n")
        fh.close()

    def transform_path(self, path):
        return ";".join(map(self.convert_lp_delivery_or_run_id, path.split("/")))

    def convert_lp_delivery_or_run_id(self, string):
        if self.is_sample(string):
            string = self.replace_sample(string)
        if self.is_delivery(string):
            string = self.replace_delivery(string)
        if self.is_integer(string):
            return "EXECUTION"
        if self.is_genotyping_array_red(string):
            string = self.replace_genotyping_array_red(string)
        if self.is_genotyping_array_green(string):
            string = self.replace_genotyping_array_green(string)
        if self.is_genotyping_array_gtc(string):
            string = self.replace_genotyping_array_gtc(string)
        string = self.replace_cancer_sample(string)
        string = self.replace_normal_sample(string)
        return string

    @staticmethod
    def is_integer(string):
        try:
            int(string)
            return True
        except ValueError:
            return False

    def is_genotyping_array_red(self, string):
        return re.match(self.genotyping_array_idat_red_regex, string)

    def replace_genotyping_array_red(self, string):
        return re.sub(self.genotyping_array_idat_red_regex,
                      self.genotyping_array_idat_red_sub,
                      string)

    def is_genotyping_array_green(self, string):
        return re.match(self.genotyping_array_idat_green_regex, string)

    def replace_genotyping_array_green(self, string):
        return re.sub(self.genotyping_array_idat_green_regex,
                      self.genotyping_array_idat_green_sub,
                      string)

    def replace_normal_sample(self, string):
        return re.sub(self.normal_sample_regex, self.normal_sample_sub, string)

    def replace_cancer_sample(self, string):
        return re.sub(self.cancer_sample_regex, self.cancer_sample_sub, string)

    def is_genotyping_array_gtc(self, string):
        return re.match(self.genotyping_array_gtc_regex, string)

    def replace_genotyping_array_gtc(self, string):
        return re.sub(self.genotyping_array_gtc_regex,
                      self.genotyping_array_gtc_sub,
                      string)

    def replace_delivery(self, string):
        return re.sub(self.delivery_regex, self.delivery_sub, string)

    def replace_sample(self, string):
        return re.sub(self.sample_regex, self.sample_sub, string)

    def is_sample(self, string):
        return re.match(self.sample_regex, string)

    def is_delivery(self, string):
        return re.match(self.delivery_regex, string)
