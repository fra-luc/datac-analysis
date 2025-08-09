import csv
import functools
import glob
from pathlib import Path

import pandas
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2
from tqdm import tqdm


def deep_getattr(obj: object, attr: str):
    return functools.reduce(getattr, attr.split("."), obj)


def read_vehicle_positions_files(vehicle_positions_files: list[str], schema: list[str]):
    for vehicle_positions_file in tqdm(vehicle_positions_files):
        feed = gtfs_realtime_pb2.FeedMessage()
        with open(vehicle_positions_file, "rb") as fp:
            feed_string_content = fp.read()
        try:
            feed.ParseFromString(feed_string_content)
        except DecodeError as _:
            tqdm.write(f"{vehicle_positions_file} parsing failed!")
            continue
        for entity in feed.entity:
            yield [deep_getattr(entity, field) for field in schema]


if __name__ == "__main__":
    VEHICLE_POSITIONS_FILES = glob.glob("downloads/vehicle_positions/*")
    VEHICLE_POSITIONS_SCHEMA = [
        "is_deleted",
        "vehicle.trip.trip_id",
        "vehicle.trip.route_id",
        "vehicle.trip.direction_id",
        "vehicle.trip.start_time",
        "vehicle.trip.start_date",
        "vehicle.vehicle.id",
        "vehicle.vehicle.label",
        "vehicle.position.latitude",
        "vehicle.position.longitude",
        "vehicle.position.odometer",
        "vehicle.current_stop_sequence",
        "vehicle.stop_id",
        "vehicle.current_status",
        "vehicle.timestamp",
    ]
    CSV_PATH = "downloads/vehicle_positions.csv"
    # with open(CSV_PATH, 'w', newline='') as csvfile:
    #     csv_writer = csv.writer(csvfile)
    #     csv_writer.writerow(VEHICLE_POSITIONS_SCHEMA)
    #     for vehicle_positions in read_vehicle_positions_files(VEHICLE_POSITIONS_FILES, VEHICLE_POSITIONS_SCHEMA):
    #         csv_writer.writerow(vehicle_positions)

    # TODO improve schema management!
    vehicle_positions = pandas.read_csv(CSV_PATH)
    (
        vehicle_positions.assign(
            **{
                "vehicle.timestamp": pandas.to_datetime(vehicle_positions["vehicle.timestamp"], unit="s"),
                "vehicle.trip.route_id": vehicle_positions["vehicle.trip.route_id"].astype("category"),
                "vehicle.trip.start_date": vehicle_positions["vehicle.trip.start_date"].astype("category"),
            }
        )
        .set_index("vehicle.timestamp")
        .sort_index()
        .to_parquet(Path("downloads/vehicle_positions.parquet"))
    )
