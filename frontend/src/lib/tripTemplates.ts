function daysAgo(n: number, hour = 8, minute = 0): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  d.setHours(hour, minute, 0, 0);
  return d.toISOString().slice(0, 19);
}

const d1b = (h: number, m: number) => daysAgo(1, h, m);

export const tripTemplates: Record<string, Record<string, unknown>> = {
  yellow: {
    service_id: "yellow",
    pickup_datetime: d1b(8, 30),
    dropoff_datetime: d1b(8, 45),
    pu_location_id: 237,
    do_location_id: 239,
    vendor_id: 2,
    ratecode_id: 1,
    payment_type_id: 1,
    passenger_count: 1,
    trip_distance: 3.5,
    fare_amount: 12.50,
    extra: 0.50,
    mta_tax: 0.50,
    tip_amount: 3.00,
    tolls_amount: 0,
    improvement_surcharge: 0.30,
    total_amount: 16.80,
    congestion_surcharge: 2.50,
    airport_fee: 0,
  },
  green: {
    service_id: "green",
    pickup_datetime: d1b(9, 0),
    dropoff_datetime: d1b(9, 15),
    pu_location_id: 41,
    do_location_id: 42,
    vendor_id: 2,
    ratecode_id: 1,
    payment_type_id: 2,
    passenger_count: 2,
    trip_distance: 2.1,
    fare_amount: 8.75,
    extra: 0,
    mta_tax: 0.50,
    tip_amount: 0,
    tolls_amount: 0,
    improvement_surcharge: 0.30,
    total_amount: 9.55,
    congestion_surcharge: 0,
  },
  fhv: {
    service_id: "fhv",
    dispatching_base_num: "B00001",
    pickup_datetime: d1b(14, 0),
    dropoff_datetime: d1b(14, 30),
    pu_location_id: 264,
    do_location_id: 265,
  },
  fhvhv: {
    service_id: "fhvhv",
    hvfhs_license_num: "HV0002",
    request_datetime: d1b(18, 0),
    pickup_datetime: d1b(18, 5),
    dropoff_datetime: d1b(18, 25),
    pu_location_id: 230,
    do_location_id: 236,
    trip_miles: 4.2,
    base_passenger_fare: 15.00,
    tolls_amount: 0,
    congestion_surcharge: 2.50,
    airport_fee: 0,
    tips: 3.00,
    driver_pay: 12.00,
    shared_request_flag: "N",
    shared_match_flag: "N",
  },
};
