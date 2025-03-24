import uuid
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER, NVARCHAR, NTEXT, BIT, DATETIMEOFFSET, INTEGER, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime

from ..configurations.config import DB_CONFIG

Base = declarative_base()

main_table = DB_CONFIG['main_table']
metric_table = DB_CONFIG['metric_table']

class OutscraperLocation(Base):
    __tablename__ = main_table

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, default=uuid.uuid4)
    MetricId = Column(UNIQUEIDENTIFIER, ForeignKey(f"{metric_table}.Id"), nullable=True)
    PlaceId = Column(NVARCHAR(255), nullable=True)
    GoogleId = Column(NVARCHAR(255), nullable=True)
    Name = Column(NVARCHAR(1000), nullable=True)
    Type = Column(NVARCHAR(255), nullable=True)
    Phone = Column(NVARCHAR(255), nullable=True)
    FullAddress = Column(NVARCHAR(4000), nullable=True)
    PostalCode = Column(NVARCHAR(10), nullable=True)
    State = Column(NVARCHAR(255), nullable=True)
    Latitude = Column(DECIMAL(19,4), nullable=True)
    Longitude = Column(DECIMAL(19,4), nullable=True)
    Verified = Column(BIT, nullable=True)
    LocationLink = Column(NTEXT, nullable=True)
    Country = Column(NVARCHAR(255), nullable=True)
    CountryCode = Column(NVARCHAR(10), nullable=True)
    Timezone = Column(NVARCHAR(255), nullable=True)

    metrics = relationship("OutscraperLocationMetric",
                          primaryjoin="OutscraperLocation.Id == OutscraperLocationMetric.LocationId",
                          back_populates="location")

    latest_metric = relationship("OutscraperLocationMetric",
                               primaryjoin="OutscraperLocation.MetricId == OutscraperLocationMetric.Id",
                               uselist=False,
                               foreign_keys=[MetricId])

    def __repr__(self):
        return f"<OutscraperLocation(Id={self.Id}, Name={self.Name})>"

class OutscraperLocationMetric(Base):
    __tablename__ = metric_table

    Id = Column(UNIQUEIDENTIFIER, primary_key=True, default=uuid.uuid4)
    LocationId = Column(UNIQUEIDENTIFIER, ForeignKey(f"{main_table}.Id"), nullable=True)
    Rating = Column(DECIMAL(19,4), nullable = True)
    Reviews = Column(INTEGER, nullable=True)
    ReviewsPerScore1 = Column(INTEGER, nullable=True)
    ReviewsPerScore2 = Column(INTEGER, nullable=True)
    ReviewsPerScore3 = Column(INTEGER, nullable=True)
    ReviewsPerScore4 = Column(INTEGER, nullable=True)
    ReviewsPerScore5 = Column(INTEGER, nullable=True)
    PhotosCount = Column(INTEGER, nullable=True)
    CreateDate = Column(DATETIMEOFFSET(7), nullable=True, default = datetime.datetime.utcnow)

    location = relationship("OutscraperLocation",
                          primaryjoin="OutscraperLocationMetric.LocationId == OutscraperLocation.Id",
                          back_populates="metrics",
                          foreign_keys=[LocationId])

    referenced_by = relationship("OutscraperLocation",
                               primaryjoin="OutscraperLocationMetric.Id == OutscraperLocation.MetricId",
                               uselist=False,
                               foreign_keys=[OutscraperLocation.MetricId],
                               overlaps="location,metrics,latest_metric")

    def __repr__(self):
        return f"<OutscraperLocationMetric(Id={self.Id}, LocationId={self.LocationId}, Rating={self.Rating})>"