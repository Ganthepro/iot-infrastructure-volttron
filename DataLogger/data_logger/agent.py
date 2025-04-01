"""
Agent documentation goes here.
"""

__docformat__ = 'reStructuredText'

import logging
import sys
from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent, Core, RPC
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, UUID
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.engine import URL
import uuid
from datetime import datetime as dt
import pytz

_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "0.1"

url = URL.create(
    drivername="postgresql",
    username="postgres",
    host="localhost",
    database="postgres",
    password="ganza112"
)
engine = create_engine(url)
Base = declarative_base()

class RawData(Base):
    __tablename__ = 'raw_data'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_id = Column(String())
    timestamp = Column(Integer(), nullable=False)
    datetime = Column(TIMESTAMP, nullable=False)
    datapoint = Column(String(), nullable=False)
    value = Column(String(), nullable=False)

    def to_dict(self):
        """Convert the model instance to a dictionary with custom formatting."""
        return {
            'id': self.id,  # assuming you have these fields
            'timestamp': str(self.timestamp),  # convert datetime to string
            'datetime': str(self.datetime),
            'datapoint': self.datapoint,
            'value': self.value,
            'device_id': self.device_id
            # add other fields as needed
        }

def data_logger(config_path, **kwargs):
    """
    Parses the Agent configuration and returns an instance of
    the agent created using that configuration.

    :param config_path: Path to a configuration file.
    :type config_path: str
    :returns: DataLogger
    :rtype: DataLogger
    """
    try:
        config = utils.load_config(config_path)
    except Exception:
        config = {}

    if not config:
        _log.info("Using Agent defaults for starting configuration.")

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    setting1 = int(config.get('setting1', 1))
    setting2 = config.get('setting2', "some/random/topic")

    return DataLogger(setting1, setting2, session, **kwargs)


class DataLogger(Agent):
    """
    Document agent constructor here.
    """

    def __init__(self, setting1=1, setting2="payload", session=sessionmaker(bind=engine)(), **kwargs):
        super(DataLogger, self).__init__(**kwargs)
        _log.debug("vip_identity: " + self.core.identity)

        self.__session = session

        self.setting1 = setting1
        self.setting2 = setting2

        self.default_config = {"setting1": setting1,
                               "setting2": setting2}

        # Set a default configuration to ensure that self.configure is called immediately to setup
        # the agent.
        self.vip.config.set_default("config", self.default_config)
        # Hook self.configure up to changes to the configuration file "config".
        self.vip.config.subscribe(self.configure, actions=["NEW", "UPDATE"], pattern="config")

    def insert(self, body):
        datetime_str = body['datetime']
        date_time = dt.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f")
        timestamp = int(date_time.replace(tzinfo=pytz.UTC).timestamp())
        datetime_timestamptz = date_time.replace(tzinfo=pytz.UTC)

        temperature = RawData(
            device_id=body["id"],
            timestamp=timestamp,
            datetime=datetime_timestamptz,
            datapoint="temperature",
            value=body["temperature"]
        )
        humidity = RawData(
            device_id=body["id"],
            timestamp=timestamp,
            datetime=datetime_timestamptz,
            datapoint="humidity",
            value=body["humidity"]
        )
        co2 = RawData(
            device_id=body["id"],
            timestamp=timestamp,
            datetime=datetime_timestamptz,
            datapoint="co2",
            value=body["co2"]
        )
        self.__session.add(temperature)
        self.__session.add(humidity)
        self.__session.add(co2)
        self.__session.commit()
        data = self.__session.query(RawData).all()[-1]
        data_dict = data.to_dict()
        _log.info("Query: {}".format(data_dict))


    def configure(self, config_name, action, contents):
        """
        Called after the Agent has connected to the message bus. If a configuration exists at startup
        this will be called before onstart.

        Is called every time the configuration in the store changes.
        """
        config = self.default_config.copy()
        config.update(contents)

        _log.debug("Configuring Agent")

        try:
            setting1 = int(config["setting1"])
            setting2 = str(config["setting2"])
        except ValueError as e:
            _log.error("ERROR PROCESSING CONFIGURATION: {}".format(e))
            return

        self.setting1 = setting1
        self.setting2 = setting2
        _log.info("Setting1: {}".format(self.setting1))
        _log.info("Setting2: {}".format(self.setting2))

        self._create_subscriptions("payload")

    def _create_subscriptions(self, topic):
        """
        Unsubscribe from all pub/sub topics and create a subscription to a topic in the configuration which triggers
        the _handle_publish callback
        """
        self.vip.pubsub.unsubscribe("pubsub", None, None)

        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=topic,
                                  callback=self._handle_publish)

    def _handle_publish(self, peer, sender, bus, topic, headers, message):
        self.insert(message)

    @Core.receiver("onstart")
    def onstart(self, sender, **kwargs):
        """
        This is method is called once the Agent has successfully connected to the platform.
        This is a good place to setup subscriptions if they are not dynamic or
        do any other startup activities that require a connection to the message bus.
        Called after any configurations methods that are called at startup.

        Usually not needed if using the configuration store.
        """
        # Example publish to pubsub
        self.vip.pubsub.publish('pubsub', "some/random/topic", message="HI!")

        # Example RPC call
        # self.vip.rpc.call("some_agent", "some_method", arg1, arg2)
        pass

    @Core.receiver("onstop")
    def onstop(self, sender, **kwargs):
        """
        This method is called when the Agent is about to shutdown, but before it disconnects from
        the message bus.
        """
        pass

    @RPC.export
    def rpc_method(self, arg1, arg2, kwarg1=None, kwarg2=None):
        """
        RPC method

        May be called from another agent via self.core.rpc.call
        """
        return self.setting1 + arg1 - arg2


def main():
    """Main method called to start the agent."""
    utils.vip_main(data_logger, 
                   version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
