# Extracted WXT and AQT production dataset

## Status

The combined WXT and AQT Level 0 Parquet extraction completed successfully on
2026-08-26. All requested dates were exported and finalization completed with
status `complete`.

```text
Dataset:        wxt-aqt-production-v5
Requested UTC:  2023-05-05 through 2025-12-16
Completed days: 957 of 957
Completed shards: 138
Errors:         0
Quarantined:    0 rows
Review required: no
```

HPC dataset root:

```text
/nfs/gce/projects/crocus-server-admins/data-rework/crocus-rework-output/wxt-aqt-production-v5
```

## Contents

The dataset contains 55,038,045,135 long-format fact rows in Hive-partitioned
Parquet:

| Sensor | VSNs | Instruments | Variables | Fact rows | Actual timestamp coverage |
| --- | ---: | ---: | ---: | ---: | --- |
| `vaisala-wxt536` | 22 | 22 | 11 | 54,787,131,902 | 2023-05-05T03:30:10.267826229Z to 2025-12-16T20:14:11.123246352Z |
| `vaisala-aqt530` | 15 | 15 | 12 | 250,913,233 | 2023-05-05T03:33:05.096190631Z to 2025-12-16T20:15:13.458363039Z |
| **Total** | **37** | **37** | **23** | **55,038,045,135** | — |

WXT variables cover environment, hail, heater, rain, supply voltage, wind
direction, and wind speed measurements. AQT variables cover environment,
gases, instrument datetime and uptime, and PM1, PM2.5, and PM10.

The final catalog contains:

- 23 sensor-level variables
- 383 instrument-variable combinations
- 1,756 deterministic series
- 0 metadata conflicts

## Layout and records

Facts use the Hive layout:

```text
facts/sensor=<sensor>/vsn=<VSN>/instrument=<instrument_id>/date=YYYY-MM-DD/part-*.parquet
```

Each fact preserves the nanosecond UTC timestamp, sensor, VSN, instrument ID,
measurement, field, typed value, and deterministic series ID. Complete retained
Influx tags are stored once in the `_series` Parquet metadata rather than being
repeated in every fact row.

The authoritative completion manifest is:

```text
export_runs/20260826T212230Z-b5c1eb0115.json
```

The `_catalog` directory contains sensor, instrument, variable,
instrument-variable, series, and metadata-conflict CSV files. The
`metadata_conflicts.csv` file contains only its header because no conflicts
were found.

## Extraction performance

The initial extraction used eight shard workers and took 141 hours, 20 minutes,
17 seconds. Peak resident memory was approximately 2.76 GiB. A timestamp
conversion error occurred only during final catalog creation; no fact data was
lost. After applying the catalog fix, the resume skipped every completed shard
and finalized successfully in 28.78 seconds with an exit status of zero.

This immutable Parquet dataset is the Level 0 source for subsequent QA/QC,
native Level 1, and one-minute and fifteen-minute Level 2 products.

## Variable inventory

These ranges are the earliest and latest timestamps represented by each
variable across the extracted dataset. They are metadata coverage ranges,
not observed value minima and maxima. Row counts reconcile with the final
selected-variable catalog.

### AQT variables

| Measurement | Field | Description | Units | VSNs | Rows | Start UTC | End UTC |
| --- | --- | --- | --- | ---: | ---: | --- | --- |
| `aqt.env.humidity` | value | Ambient Relative Humidity | percent relative humidity | 15 | 20,909,442 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.env.pressure` | value | Ambient Atmospheric Pressure | hPa | 15 | 20,909,441 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.env.temp` | value | Ambient Temperature | degrees Celsius | 15 | 20,909,447 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.gas.co` | value | Carbon Monoxide Gas Concentration | ppm | 15 | 20,909,435 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.gas.no` | value | Nitric Oxide Gas Concentration | ppm | 15 | 20,909,436 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.gas.no2` | value | Nitrogen Dioxide Gas Concentration | ppm | 15 | 20,909,439 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.gas.ozone` | value | Ozone Gas Concentration | ppm | 15 | 20,909,433 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.house.datetime` | value | UTC time in YYYY-MM-DDTHH:MM:SS format | UTC time | 15 | 20,909,429 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.house.uptime` | value | Time in seconds since instrument startup | seconds | 15 | 20,909,428 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.particle.pm1` | value | Particulate Matter less than 1 microns in diameter | microgram per cubic meter | 15 | 20,909,436 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.particle.pm10` | value | Particulate Matter less than 10 microns in diameter | microgram per cubic meter | 15 | 20,909,433 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |
| `aqt.particle.pm2.5` | value | Particulate Matter less than 2.5 microns in diameter | microgram per cubic meter | 15 | 20,909,434 | 2023-05-05T03:33:05.096190631Z | 2025-12-16T20:15:13.458363039Z |

### WXT variables

| Measurement | Field | Description | Units | VSNs | Rows | Start UTC | End UTC |
| --- | --- | --- | --- | ---: | ---: | --- | --- |
| `wxt.env.humidity` | value | Not supplied | percent | 22 | 6,009,703,719 | 2023-05-05T03:30:10.267826229Z | 2025-12-16T20:14:11.123246352Z |
| `wxt.env.pressure` | value | Not supplied | hectoPascal | 22 | 6,009,703,550 | 2023-05-05T03:30:10.267826229Z | 2025-12-16T20:14:11.123246352Z |
| `wxt.env.temp` | value | Not supplied | degree Celsius | 22 | 6,009,704,036 | 2023-05-05T03:30:10.267826229Z | 2025-12-16T20:14:11.123246352Z |
| `wxt.hail.accumulation` | value | Not supplied | hits per square centimeter | 10 | 1,562,254,433 | 2023-05-07T02:21:21.499867054Z | 2025-12-16T20:01:57.860974712Z |
| `wxt.heater.status` | value | Not supplied | unitless | 18 | 4,707,271,864 | 2024-10-22T20:21:27.084569012Z | 2025-12-16T20:14:11.123246352Z |
| `wxt.heater.temp` | value | Not supplied | degree Celsius | 21 | 6,009,376,932 | 2023-05-05T03:30:10.267826229Z | 2025-12-16T20:14:11.123246352Z |
| `wxt.heater.volt` | value | Not supplied | volts | 21 | 6,009,376,762 | 2023-05-05T03:30:10.267826229Z | 2025-12-16T20:14:11.123246352Z |
| `wxt.rain.accumulation` | value | Not supplied | milimeters | 22 | 6,010,976,192 | 2023-05-05T03:30:10.267826229Z | 2025-12-16T20:14:11.123246352Z |
| `wxt.voltage.supply` | value | Not supplied | volts | 1 | 433,027,607 | 2024-06-20T22:49:31.596996864Z | 2025-12-16T20:01:57.860974712Z |
| `wxt.wind.direction` | value | Not supplied | degrees | 22 | 6,012,868,573 | 2023-05-05T03:30:10.267826229Z | 2025-12-16T20:14:11.123246352Z |
| `wxt.wind.speed` | value | Not supplied | meters per second | 22 | 6,012,868,234 | 2023-05-05T03:30:10.267826229Z | 2025-12-16T20:14:11.123246352Z |

## VSN and instrument coverage

Each VSN maps to one resolved instrument identity within each sensor type.
Coverage is the interval between the first and last extracted timestamp; it
does not imply that every point inside the interval is present.

### AQT VSN coverage

| VSN | Instrument ID | Variables | Series | Rows | Start UTC | End UTC | Coverage days |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: |
| `W039` | `W039--vaisala-aqt530--core--5b8d3ce9a8bc` | 12 | 24 | 6,108 | 2023-05-05T03:33:05.096190631Z | 2023-05-05T12:15:04.599601708Z | 0.362 |
| `W069` | `W069--vaisala-aqt530--core--7d3281693666` | 12 | 24 | 13,073,943 | 2025-03-08T00:22:23.633541931Z | 2025-12-12T19:45:13.367572623Z | 279.808 |
| `W08B` | `W08B--vaisala-aqt530--core--32060d89ddde` | 12 | 36 | 19,961,471 | 2024-09-25T14:33:51.513512900Z | 2025-12-12T19:08:48.382265541Z | 443.191 |
| `W08D` | `W08D--vaisala-aqt530--core--df6b0090a23b` | 12 | 96 | 37,725,560 | 2023-05-05T15:40:15.011519287Z | 2025-12-16T20:14:30.980109155Z | 956.19 |
| `W08E` | `W08E--vaisala-aqt530--core--0f892acf70d0` | 12 | 108 | 32,435,393 | 2023-12-13T19:00:22.715201529Z | 2025-12-16T20:14:54.717422807Z | 734.052 |
| `W095` | `W095--vaisala-aqt530--core--e163084cea0c` | 12 | 24 | 13,220,039 | 2025-03-14T17:10:10.829256060Z | 2025-12-16T20:15:00.015917516Z | 277.128 |
| `W096` | `W096--vaisala-aqt530--core--3f57d37437e3` | 12 | 48 | 26,357,539 | 2024-06-21T01:50:32.763882104Z | 2025-12-16T20:15:13.458363039Z | 543.767 |
| `W098` | `W098--vaisala-aqt530--core--b0f19aecf20a` | 12 | 24 | 1,864,134 | 2025-04-18T01:30:26.039965466Z | 2025-06-06T04:44:12.266297458Z | 49.135 |
| `W099` | `W099--vaisala-aqt530--core--94b6cd87b5de` | 12 | 60 | 37,486,629 | 2023-12-05T22:11:05.871946991Z | 2025-12-16T20:15:13.135404615Z | 741.92 |
| `W09D` | `W09D--vaisala-aqt530--core--af3057623701` | 12 | 36 | 14,646,248 | 2025-02-25T18:14:28.306134896Z | 2025-12-12T19:38:20.543432103Z | 290.058 |
| `W09E` | `W09E--vaisala-aqt530--core--5dc161afeff8` | 12 | 36 | 13,416,623 | 2024-07-23T00:32:34.305154466Z | 2025-12-12T19:34:58.322174360Z | 507.793 |
| `W09F` | `W09F--vaisala-aqt530--core--b4389df0426d` | 12 | 12 | 7,716 | 2024-07-26T14:17:14.559045370Z | 2025-02-27T23:56:09.422855958Z | 216.402 |
| `W0A0` | `W0A0--vaisala-aqt530--core--895f71263bd4` | 12 | 24 | 17,947,962 | 2024-11-19T23:20:20.853962916Z | 2025-12-16T16:43:25.063154587Z | 391.724 |
| `W0A1` | `W0A1--vaisala-aqt530--core--7655b8cfb9bf` | 12 | 24 | 14,152,604 | 2025-02-21T00:20:15.333823684Z | 2025-12-16T20:14:39.612208165Z | 298.829 |
| `W0A4` | `W0A4--vaisala-aqt530--core--15eaee7f9137` | 12 | 60 | 8,611,264 | 2024-06-27T20:40:28.996781283Z | 2025-12-16T20:14:04.911607688Z | 536.982 |

### WXT VSN coverage

| VSN | Instrument ID | Variables | Series | Rows | Start UTC | End UTC | Coverage days |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: |
| `W01B` | `W01B--vaisala-wxt536--core--1b06dce7ed4d` | 9 | 36 | 2,455,527,068 | 2023-10-19T00:37:10.409109701Z | 2025-12-12T18:41:17.225679473Z | 785.753 |
| `W039` | `W039--vaisala-wxt536--core--8b10123cd91c` | 8 | 8 | 3,021,256 | 2023-05-05T03:30:10.267826229Z | 2023-05-05T12:15:09.141206740Z | 0.365 |
| `W057` | `W057--vaisala-wxt536--core--81ca8e85f911` | 9 | 46 | 79,241,027 | 2023-05-06T19:20:19.084974283Z | 2023-08-31T13:05:27.222850347Z | 116.74 |
| `W067` | `W067--vaisala-wxt536--core--96608f5c339f` | 10 | 67 | 354,754,543 | 2023-10-19T01:01:30.357066099Z | 2025-12-16T20:10:49.338774176Z | 789.798 |
| `W069` | `W069--vaisala-wxt536--core--862be7fe459e` | 7 | 24 | 7,316,497 | 2024-10-07T22:21:46.714743220Z | 2025-12-01T01:33:27.521163205Z | 419.133 |
| `W06A` | `W06A--vaisala-wxt536--core--f6c1fd64a63b` | 9 | 36 | 1,504,243,062 | 2024-10-21T14:00:08.404017226Z | 2025-10-24T19:02:59.917038370Z | 368.21 |
| `W06C` | `W06C--vaisala-wxt536--core--4fcd0ec203d1` | 9 | 60 | 2,625,646,591 | 2024-11-20T03:50:09.903389402Z | 2025-12-16T20:14:11.123246352Z | 391.683 |
| `W071` | `W071--vaisala-wxt536--core--463d13083d94` | 9 | 18 | 1,836,946,600 | 2025-02-24T21:30:08.113971505Z | 2025-10-07T22:39:03.749070761Z | 225.048 |
| `W08B` | `W08B--vaisala-wxt536--core--42f10dc622f2` | 9 | 74 | 3,444,183,324 | 2024-09-25T14:31:52.545926426Z | 2025-12-12T19:08:58.808392570Z | 443.192 |
| `W08D` | `W08D--vaisala-wxt536--core--934c67f6166a` | 9 | 154 | 6,150,739,142 | 2023-05-05T16:20:07.661126241Z | 2025-12-16T20:00:50.995330836Z | 956.153 |
| `W08E` | `W08E--vaisala-wxt536--core--e79753d1a123` | 9 | 131 | 4,117,030,359 | 2023-05-19T17:10:08.234231813Z | 2025-12-16T20:01:31.137442911Z | 942.119 |
| `W095` | `W095--vaisala-wxt536--core--792d997c77e2` | 10 | 33 | 2,627,296,648 | 2025-03-14T17:10:11.879108393Z | 2025-12-16T20:01:21.676338731Z | 277.119 |
| `W096` | `W096--vaisala-wxt536--core--cbc808667e24` | 11 | 102 | 4,677,125,543 | 2024-06-20T22:49:31.596996864Z | 2025-12-16T20:01:57.860974712Z | 543.884 |
| `W097` | `W097--vaisala-wxt536--core--649ca0b6f6e4` | 9 | 20 | 2,414,315,755 | 2025-01-31T20:50:12.295353816Z | 2025-12-16T20:13:35.762406051Z | 318.975 |
| `W098` | `W098--vaisala-wxt536--core--aa22a8ace90f` | 10 | 32 | 238,050,683 | 2025-04-17T22:57:26.670451516Z | 2025-06-06T04:44:30.395179013Z | 49.241 |
| `W099` | `W099--vaisala-wxt536--core--6c922d091b8d` | 9 | 70 | 6,367,106,700 | 2023-12-05T22:10:10.776166674Z | 2025-12-16T20:02:05.179741914Z | 741.911 |
| `W09D` | `W09D--vaisala-wxt536--core--23a9906b4c45` | 10 | 32 | 2,940,841,702 | 2025-02-25T18:12:28.627559571Z | 2025-12-12T19:38:36.696347158Z | 290.06 |
| `W09E` | `W09E--vaisala-wxt536--core--d09cea632c7b` | 10 | 31 | 2,633,312,391 | 2024-07-26T21:28:27.657636776Z | 2025-12-12T19:35:11.018725509Z | 503.921 |
| `W09F` | `W09F--vaisala-wxt536--core--15bba9089188` | 9 | 9 | 109,016 | 2024-07-26T14:07:59.709853266Z | 2024-07-26T17:48:16.707296397Z | 0.153 |
| `W0A0` | `W0A0--vaisala-wxt536--core--3981956aa520` | 9 | 49 | 3,357,907,277 | 2024-11-19T23:20:13.218269796Z | 2025-12-16T16:43:27.094117281Z | 391.724 |
| `W0A1` | `W0A1--vaisala-wxt536--core--ee3274a084ad` | 10 | 33 | 2,838,193,809 | 2025-02-21T00:20:06.376394813Z | 2025-12-16T20:01:06.370145360Z | 298.82 |
| `W0A4` | `W0A4--vaisala-wxt536--core--354abfb39272` | 9 | 55 | 4,114,222,909 | 2024-06-27T20:30:06.756726708Z | 2025-12-16T20:01:00.884959309Z | 536.98 |
