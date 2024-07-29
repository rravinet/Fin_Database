import os
import logging

def setup_logging():
    root = os.path.dirname(os.path.abspath(__file__))

    # Set the logs directory within your new project path
    logs_path = os.path.join(root, 'logs')
    os.makedirs(logs_path, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(logs_path, 'fin_database.log'),
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(filename)s - %(lineno)d]'
    )

    sqlalchemy_engine_logger = logging.getLogger('sqlalchemy.engine')
    sqlalchemy_engine_logger.setLevel(logging.WARNING)
