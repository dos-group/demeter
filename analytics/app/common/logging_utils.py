import logging
import os
from importlib import reload
from datetime import datetime

from pydantic import BaseModel

from app.common.configuration import GeneralSettings
from app.optimization.schemes import EvaluationRequest


def log_request(request: BaseModel):
    if isinstance(request, EvaluationRequest):
        log_s: str = ", ".join([f"{k}={len(getattr(request, k))}" if \
            isinstance(getattr(request, k), list) else \
            f"{k}={getattr(request, k)}" for k in list(request.__fields__.keys())])
        logging.info(f"request=[{log_s}]")
    else:
        logging.info(f"request=[{request}]")


def init_logging(do_reload: bool = True):
    # get the pid for the current process
    pid = os.getpid()
    general_settings: GeneralSettings = GeneralSettings.get_instance()
    logging_level = general_settings.logging_level
    log_file_name: str = f"{general_settings.app_env}_{datetime.now().isoformat()}-pid={pid}.log"
    log_file_name = os.path.join(general_settings.log_dir, log_file_name)

    if do_reload:
        reload(logging)
    logging.basicConfig(
        level=getattr(logging, logging_level.upper()),
        format="%(asctime)s [%(levelname)s] [%(process)d] %(message)s [%(module)s.%(funcName)s]__[L%(lineno)d]]",
        handlers=[el for el in [
            logging.StreamHandler(),
            logging.FileHandler(log_file_name) if log_file_name is not None else None
        ] if el is not None]
    )

    logging.info(f"Successfully initialized logging.")
