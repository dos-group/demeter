import asyncio
import copy
import logging
import math
import os
import pathlib
import sys
from typing import Tuple, List
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

parent_dir = str(pathlib.Path(__file__).parents[1].absolute())
os.environ["PYTHONPATH"] = parent_dir + ":" + os.environ.get("PYTHONPATH", "")
sys.path.append(parent_dir)

from app.common.configuration import GeneralSettings
from app.common.logging_utils import init_logging
from app.common.models import BasePredictionModel
from app.common.schemes import JobModel
from app.workload.models import WorkloadModelImpl, WorkloadModelProvider
from app.workload.schemes import WorkloadModelTrainingRequest, TimeSeries, WorkloadModelPredictionRequest

general_settings: GeneralSettings = GeneralSettings().get_instance()


def _get_test_random_data() -> Tuple[TimeSeries, TimeSeries, TimeSeries]:
    return TimeSeries.create_example(6000, offset=-5999), \
           TimeSeries.create_example(general_settings.workload_prediction_period_in_s,
                                     offset=1), \
           TimeSeries.create_example(general_settings.workload_prediction_period_in_s,
                                     offset=1 + general_settings.workload_prediction_period_in_s)


def _get_real_data(file_name: str, num_generators: int,
                   seconds: int, offset: int = 0) -> Tuple[TimeSeries, TimeSeries, TimeSeries]:
    path_to_file: str = os.path.join(general_settings.root_dir, file_name)
    logging.info(f"---> Use file: {path_to_file}")
    df = pd.read_csv(path_to_file, sep="|")  # 18 hours
    df["value"] *= num_generators  # num_generators simultaneously to give us our throughput rates
    if offset:
        logging.info(f"---> Dropping last {offset} rows of source DataFrame...")
        df = df.drop(df.tail(offset).index)

    train: int = seconds
    update: int = general_settings.workload_prediction_period_in_s
    test: int = general_settings.workload_prediction_period_in_s

    concat_df = pd.concat([df] * math.ceil((train + update + test) / len(df)))  # repeat X times to get more data

    timestamps = np.arange(train + update + test)
    values = concat_df["value"].values.reshape(-1)[:(train + update + test)]
    # add some noise
    np.random.seed(42)
    values = np.abs(values + (values * np.random.normal(0, 0.01, len(values))))

    logging.info(f"---> min={np.amin(values)}, max={np.amax(values)}")

    return TimeSeries.fold(timestamps[:train], values[:train]), \
           TimeSeries.fold(timestamps[train:(train + update)], values[train:(train + update)]), \
           TimeSeries.fold(timestamps[-test:], values[-test:])


async def prepare_workload_models(job_names: List[str]):

    for file_name in os.listdir(general_settings.models_dir):
        if "_raw_" not in file_name:
            os.remove(os.path.join(general_settings.models_dir, file_name))

    for job_name in job_names:
        routes = ["profile", "rescale"]
        for route in routes:
            logging.info(f"Prepare workload-model for '{job_name}' job and '{route}' route...")

            if route == routes[0]:
                workload_model: BasePredictionModel = WorkloadModelProvider.get_instance(f"workload_{route}").__call__(JobModel(job=job_name))
                if workload_model.is_fitted:
                    logging.info(f"Workload-model for '{job_name}' job and '{route}' route prepared.")
                    continue

                train_data: TimeSeries
                update_data: TimeSeries
                test_data: TimeSeries

                if job_name == "ADS":
                    train_data, update_data, test_data = _get_real_data("ADS_3D_1S_4_sub_30K_max.csv",
                                                                        3,
                                                                        63200 * 2, offset=1600)
                elif job_name == "CARS":
                    train_data, update_data, test_data = _get_real_data("CARS_3D_1S_4_sub_35K_max.csv",
                                                                        3,
                                                                        21600 * 6)
                elif job_name == "TEST":
                    train_data, update_data, test_data = _get_test_random_data()
                else:
                    raise ValueError(f"Unknown job name '{job_name}'!")

                logging.info(f"---> Start training with {train_data.count} samples...")
                workload_model.fit(WorkloadModelTrainingRequest(**{
                    "workload": train_data.dict(),
                    "job": job_name
                }))
                logging.info("---> Training finished.")

                for other_route in routes[1:]:
                    # copy the trained model, so that all routes have same initial model
                    workload_model_temp_copy: WorkloadModelImpl = copy.deepcopy(workload_model)
                    workload_model_temp_copy.target_path = workload_model_temp_copy.target_path.replace(f"workload_{route}",
                                                                                                        f"workload_{other_route}")
                    workload_model_temp_copy.save()

                if test_data is not None:
                    logging.info(f"---> Start testing with {test_data.count} samples (and {update_data.count} update samples)...")
                    workload_model_test_copy: WorkloadModelImpl = copy.deepcopy(workload_model)
                    response: TimeSeries = workload_model_test_copy.predict(WorkloadModelPredictionRequest(**{
                        "workload": update_data.dict(),
                        "prediction_period_in_s": test_data.count,
                        "job": job_name
                    }), save=False)

                    plt.figure(figsize=(10, 5))
                    plt.title(job_name)

                    for name, ts in zip(["update", "test"], [update_data, test_data]):
                        orig_timestamps, orig_vals = TimeSeries.unfold(ts)
                        pros_timestamps, pros_vals = TimeSeries.unfold(workload_model_test_copy._process_workload(
                            ts,
                            workload_model_test_copy.step_size,
                            workload_model_test_copy.smooth_args
                        ))

                        plt.plot(orig_timestamps, orig_vals, label=name)

                        if name == "update":
                            plt.plot(pros_timestamps, pros_vals, label=f"{name}_processed")

                        if name == "test":
                            pred_timestamps, pred_vals = TimeSeries.unfold(response)

                            plt.plot(pred_timestamps, pred_vals, label=f"{name}_pred")

                            logging.info("---> Predict-Performance of Workload-Regressor after initial training: " +
                                         workload_model_test_copy.get_regression_results(orig_vals, pred_vals))

                    plt.legend()
                    plt.tight_layout()
                    plt.show()

                    logging.info("---> Testing finished.")

            logging.info(f"Workload-model for '{job_name}' job and '{route}' route prepared.")


if __name__ == "__main__":

    general_settings: GeneralSettings = GeneralSettings.get_instance()

    init_logging()

    asyncio.get_event_loop().run_until_complete(prepare_workload_models(general_settings.job_names + ["TEST"]))

