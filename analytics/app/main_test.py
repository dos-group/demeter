import asyncio
import copy
import os
from typing import Optional

import numpy as np
import pytest as pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.common.configuration import GeneralSettings
from app.optimization.models.embedder import EmbedderBO
from app.optimization.schemes import ConfigurationModel, SearchSpaceModel
from app.prepare_model import prepare_workload_models
from app.main import start_server
from app.workload.schemes import TimeSeries

app: Optional[FastAPI] = None
client: Optional[TestClient] = None

@pytest.fixture(autouse=True)
def setup():
    global app
    global client

    def cleanup():
        for f_name in os.listdir(GeneralSettings.get_instance().models_dir):
            if "_TEST_" in f_name:
                os.remove(os.path.join(GeneralSettings.get_instance().models_dir, f_name))

    # We do not want to wait for ages
    GeneralSettings.get_instance().profiling_timeout = 20

    cleanup()
    asyncio.get_event_loop().run_until_complete(prepare_workload_models(["TEST"]))
    app = start_server()
    client = TestClient(app)

    yield

    cleanup()
    app = None
    client = None


def test_profiling():
    throughput_rate: int = 3000

    default_configs = {
        "scaleout": 20,
        "checkpoint_interval": 10000,
        "container_cpu": 1.0,
        "container_memory": 4096,
        "task_slots": 1.0
    }

    current_profile = {
        "configs": default_configs,
        "metrics": {
            "throughput_rate": throughput_rate,
            "latency": 1111,
            "recovery_time": -1
        }
    }

    search_space = {
        "min_scaleout": 4,
        "max_scaleout": 20,
        "min_checkpoint_interval": 10000,
        "max_checkpoint_interval": 90000,
        "min_container_cpu": 1.0,
        "max_container_cpu": 4.0,
        "min_container_memory": 1024,
        "max_container_memory": 4096,
        "min_task_slots": 1,
        "max_task_slots": 4
    }

    constraints = {
        "recovery_time": 180
    }
    objectives = {
        "cpu": None,
        "memory": None
    }

    response = client.post("/optimization/profile", json={
        "job": "TEST",
        "max_cluster_cpu": 50,
        "max_cluster_memory": 500000,
        "forecast_updates": TimeSeries.create_example(600).dict(),
        "profiles": [],
        "current_profile": current_profile,
        "default_configs": default_configs,
        "search_space": search_space,
        "constraints": constraints,
        "objectives": objectives
    })

    assert response.status_code == 200
    response_dict: dict = response.json()
    # in the beginning, get some random configs suggested
    profiling_suggestions = response_dict["profiling_configs"]
    assert isinstance(profiling_suggestions, list) and len(profiling_suggestions)

    embedder = EmbedderBO(SearchSpaceModel(**search_space), ConfigurationModel(**default_configs))
    options = embedder.options

    new_sobol_suggestions = []
    latencies = np.linspace(1111, 2222, num=len(profiling_suggestions))
    recovery_times = np.linspace(150, 300, num=len(profiling_suggestions))
    for suggestion, latency, recovery_time in zip(profiling_suggestions, latencies, recovery_times):
        for i in np.arange(0.9, 5.1, step=0.5):
            suggestion = copy.deepcopy(suggestion)
            suggestion["configs"] = options.pop(0).dict()
            suggestion["metrics"] = suggestion.get("metrics", {})
            suggestion["metrics"]["throughput_rate"] = throughput_rate * i
            suggestion["metrics"]["latency"] = latency * i
            suggestion["metrics"]["recovery_time"] = recovery_time * i
            suggestion.pop("acqf_value", None)
            new_sobol_suggestions.append(suggestion)

    response = client.post("/optimization/profile", json={
        "job": "TEST",
        "max_cluster_cpu": 50,
        "max_cluster_memory": 500000,
        "forecast_updates": TimeSeries.create_example(600, offset=601).dict(),
        "profiles": new_sobol_suggestions,
        "current_profile": current_profile,
        "default_configs": default_configs,
        "search_space": search_space,
        "constraints": constraints,
        "objectives": objectives
    })

    assert response.status_code == 200
    response_dict: dict = response.json()
    # now, get next suggestions
    profiling_suggestions = response_dict["profiling_configs"]
    assert isinstance(profiling_suggestions, list)
