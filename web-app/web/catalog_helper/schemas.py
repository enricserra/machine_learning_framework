class SuspiciousAttributes(Base):
    __tablename__ = "suspicious_attributes"
    id = Column(Integer, primary_key=True)
    file_path = Column(String)


class FileAttributes(Base):
    __tablename__ = "files_attrs_table"
    id = Column(Integer, primary_key=True)
    attr_id = relationship("AttributesSummary")
    file_path = Column(String)

    def __repr__(self):
        return "<FileAttributes(id='%s', attr_id='%s', file_path='%s')>" % (
            self.id, self.attr_id, self.file_path
        )


class AttributesSummary(Base):
    __tablename__ = "attributes_summary"
    id = Column(Integer, primary_key=True)
    plot_title = Column(String)
    plot_minimum = Column(Float)
    plot_maximum = Column(Float)
    plot_values = Column(String)
    is_numeric = Column(Boolean)
    file_id = relationship()

    def __repr__(self):
        return "<AttributeSummary(id='%s',plot_title='%s', plot_maximum='%s'," \
               "plot_minimum='%s', plot_values='%s', is_numeric='%s')>" % (
            self.id, self.plot_title, self.plot_minimum, self.plot_maximum,
            self.plot_values, self.is_numeric
        )
