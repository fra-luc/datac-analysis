import glob
from pathlib import Path

import pandas
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2


def read_vehicle_positions_files(vehicle_positions_files: list[str]):
    for vehicle_positions_file in vehicle_positions_files:
        feed = gtfs_realtime_pb2.FeedMessage()
        with open(vehicle_positions_file, "rb") as fp:
            feed_string_content = fp.read()
        try:
            feed.ParseFromString(feed_string_content)
        except DecodeError as _:
            print(f"{vehicle_positions_file} parsing failed!")
            continue
        for entity in feed.entity:
            yield (
                entity.is_deleted,
                entity.vehicle.trip.trip_id,
                entity.vehicle.trip.route_id,
                entity.vehicle.trip.direction_id,
                entity.vehicle.trip.start_time,
                entity.vehicle.trip.start_date,
                entity.vehicle.vehicle.id,
                entity.vehicle.vehicle.label,
                entity.vehicle.position.latitude,
                entity.vehicle.position.longitude,
                entity.vehicle.position.odometer,
                entity.vehicle.current_stop_sequence,
                entity.vehicle.stop_id,
                entity.vehicle.current_status,
                entity.vehicle.timestamp,
            )


if __name__ == "__main__":
    VEHICLE_POSITIONS_FILES = glob.glob("downloads/vehicle_positions/*")
    vehicle_positions = pandas.DataFrame(read_vehicle_positions_files(VEHICLE_POSITIONS_FILES))
    vehicle_positions.rename(
        columns={
            0: "is_deleted",
            1: "vehicle.trip.trip_id",
            2: "vehicle.trip.route_id",
            3: "vehicle.trip.direction_id",
            4: "vehicle.trip.start_time",
            5: "vehicle.trip.start_date",
            6: "vehicle.vehicle.id",
            7: "vehicle.vehicle.label",
            8: "vehicle.position.latitude",
            9: "vehicle.position.longitude",
            10: "vehicle.position.odometer",
            11: "current_stop_sequence",
            12: "vehicle.stop_id",
            13: "vehicle.current_status",
            14: "vehicle.timestamp",
        },
        inplace=True,
    )
    vehicle_positions["vehicle.timestamp"] = pandas.to_datetime(vehicle_positions["vehicle.timestamp"], unit="s")
    vehicle_positions["vehicle.trip.route_id"] = vehicle_positions["vehicle.trip.route_id"].astype('category')
    vehicle_positions["vehicle.trip.start_date"] = pandas.to_datetime(vehicle_positions["vehicle.trip.start_date"])
    vehicle_positions.set_index("vehicle.timestamp", inplace=True)
    vehicle_positions.sort_index(inplace=True)
    vehicle_positions.to_parquet(Path("data/vehicle_positions.parquet"))
