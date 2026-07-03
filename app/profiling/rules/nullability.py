NULLABLE_COLUMNS: dict[str, set[str]] = {
    "fhv": {"SR_Flag"},
    "fhvhv": {"originating_base_num", "on_scene_datetime", "shared_request_flag", "shared_match_flag", "access_a_ride_flag", "wav_request_flag", "wav_match_flag"},
    "yellow": {"airport_fee", "congestion_surcharge", "cbd_congestion_fee"},
    "green": {"ehail_fee", "congestion_surcharge", "cbd_congestion_fee"},
}
