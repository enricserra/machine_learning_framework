import os
import operator
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, sessionmaker

from schemas import AttributesSummary, FileAttributes, SuspiciousAttributes

Base = declarative_base()


class Aggregated:
    def __init__(self, name, min, max, is_numeric, outliers, distribution):
        self.name = name
        self.min = min
        self.max = max
        self.is_numeric = is_numeric
        self.outliers = outliers
        self.distribution = distribution
        self.file_path = self.get_file_path_from_name()

    def get_file_path_from_name(self):
        return self.name.split(':')[0]


class CatalogAggregator:
    def __init__(self, summary_dir, logger):
        self.summary_dir = summary_dir
        self.files_crawled = os.listdir(self.summary_dir)
        self.aggregated = {}
        self.session = self.create_session
        self.logger = logger
        self.session = self.create_session()

    def create_session(self):
        engine = create_engine("postgresql://postgres:postgres@localhost:5432/catalog_summary")
        Session = sessionmaker(bind=engine)
        return Session()

    def aggregate(self):
        for a_file in self.files_crawled:
            self.aggregate_file(a_file)
        for aggregated_file in self.aggregated:
            self.logger.info("Aggregated file {} is numeric {} number {} "
                        "with result {}".format(aggregated_file,
                        self.aggregated[aggregated_file].name,
                        str(self.aggregated[aggregated_file].is_numeric),
                        self.aggregated[aggregated_file].distribution))
            self.store_in_db(self.aggregated[aggregated_file])

    def store_in_db(self, aggregated_instance):
        this_summary = AttributesSummary(plot_title=aggregated_instance.name,
                                         plot_minimum=aggregated_instance.min,
                                         plot_maximum=aggregated_instance.max,
                                         plot_values=str(aggregated_instance.distribution),
                                         is_numeric=aggregated_instance.is_numeric)
        print "This is the id {} ".format(pk = inspect(a).identitythis_summary.id)
        file_summary = FileAttributes(attr_id=this_summary.id,
                                      file_path=aggregated_instance.file_path)

        print "Sending to summary {} ".format(this_summary)
        self.session.add(this_summary)
        self.session.add(file_summary)
        self.session.commit()

    def aggregate_file(self, a_file):
        file_path = os.path.join(self.summary_dir, a_file)
        fh = open(file_path, 'r')
        fh.readline()
        fh.readline()
        for line in fh:
            line = line.rstrip('\n')
            if not self.is_numeric(line):
                fh.close()
                return self.aggregate_text_file(file_path)
        fh.close()
        return self.aggregate_numeric_file(file_path)

    @staticmethod
    def is_numeric(value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def aggregate_text_file(self, file_path):
        fh = open(file_path, 'r')
        name = fh.readline()
        aggregated_dict = {}
        for line in fh:
            line = line.rstrip('\n')
            if line:
                if line in aggregated_dict:
                    aggregated_dict[line] += 1
                else:
                    aggregated_dict[line] = 1
        self.aggregated[name] = Aggregated(name, 0, 0, False, [],
                                           self.process_aggregated_dict(aggregated_dict))

    def process_aggregated_dict(self, aggregated_dict):
        sorted_aggregated_dict = sorted(aggregated_dict.items(),
                                        key=operator.itemgetter(1),
                                        reverse=True)
        if len(sorted_aggregated_dict) > 19:
            collapsed_dict = sorted_aggregated_dict[0:19]
            collapsed_dict.append(['Rest', 0])
            i = 19
            while i < len(sorted_aggregated_dict):
                collapsed_dict[19][1] += sorted_aggregated_dict[i][1]
                i += 1
            collapsed_dict[19] = tuple(collapsed_dict[19])
            return collapsed_dict
        return sorted_aggregated_dict

    def aggregate_numeric_file(self, file_path):
        name, values, min, max = self.parse_numeric_file(file_path)
        outliers = self.find_outliers(values, min, max)
        min, max = self.recalculate_min_max(values, outliers)
        self.aggregated[name] = Aggregated(name, min, max, True, outliers,
                                           self.distribute_in_intervals(min, max, 200, values))

    def distribute_in_intervals(self, min, max, intervals, values):
        space = [min]
        length = max - min
        counts = []
        i = 0
        while i < intervals:
            counts.append(0)
            space.append(space[len(space) - 1] + length/intervals)
            i += 1
        for value in values:
            flag = True
            i = 0
            while flag:
                if i == len(space) -1:
                    counts[len(counts) -1] += 1
                    flag = False
                elif space[i] <= value and space[i+1] >= value:
                    counts[i] += 1
                    flag=False
                i += 1
        return counts

    def find_outliers(self, values, min, max):
        return []

    @staticmethod
    def recalculate_min_max(values, outliers):
        for value in values:
            if value not in outliers:
                try:
                    if value < min:
                        min = value
                except NameError:
                    min = value
                try:
                    if value > max:
                        max = value
                except NameError:
                    max = value
        return min, max

    @staticmethod
    def parse_numeric_file(file_path):
        values =[]
        fh = open(file_path, 'r')
        name = fh.readline()
        fh.readline()
        first_line = fh.readline().rstrip('\n')
        first_line = float(first_line)
        min = first_line
        max = first_line
        values.append(first_line)
        for line in fh:
            line = float(line.rstrip('\n'))
            if line < min:
                min = line
            if line > max:
                max = line
            values.append(line)
        fh.close()
        return name, values, min, max


