# Geo Assets

## `boroughs.geojson`

NYC borough boundaries (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR) dissolved from TLC taxi zones.

### Regeneration

Requires `ogr2ogr` (GDAL). From project root:

```bash
ogr2ogr -f GeoJSON -t_srs EPSG:4326 -dialect SQLITE \
  -sql "SELECT borough, ST_Union(geometry) AS geometry FROM taxi_zones GROUP BY borough" \
  frontend/public/geo/boroughs.geojson \
  lib/shapefiles/taxi_zones/taxi_zones.shp
```

This dissolves the 263 taxi zones by `borough` field and reprojects from EPSG:2263 to WGS84.

## `zones.geojson`

NYC taxi zone boundaries (263 zones, undissolved). Each feature has `LocationID`, `zone`, and
`borough` properties — use `LocationID` to join with `pu_location_id` in mart data.

### Regeneration

```bash
ogr2ogr -f GeoJSON -t_srs EPSG:4326 \
  frontend/public/geo/zones.geojson \
  lib/shapefiles/taxi_zones/taxi_zones.shp
```

No dissolve — all 263 zones preserved as individual features, reprojected from EPSG:2263 to WGS84.
