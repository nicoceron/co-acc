# Phase 7 ingest log

| finished_at_utc | dataset | mode | status | rows | coverage | watermark | note |
|---|---|---|---:|---:|---|---|---|
| 2026-05-24T02:55:41+00:00 | `paco_sanctions` | full | ok | 54369 | pass | - | custom adapter snapshot `snapshot=20260524T025536Z`; lake reality green |
| 2026-05-16T02:34:43.560517+00:00 | `2jzx-383z` | smoke | failed | 0 | - | - | exhausted retries for https://www.datos.gov.co/resource/2jzx-383z.json: Client error '403 Forbidden' for url 'https://www.datos.gov.co/resource/2jzx-383z.json?%24select=max%28fecha_de_vinculaci_n%29' For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403 |
| 2026-05-16T02:34:46.244396+00:00 | `jbjy-vk9h` | smoke | ok | 842 | pass | 2026-05-04T00:00:00+00:00 | smoke: seeded watermark 2026-05-03T00:00:00+00:00 from max(fecha_de_firma) |
| 2026-05-16T02:34:47.561504+00:00 | `qddk-cgux` | smoke | ok | 875 | pass | 2017-12-31T00:00:00+00:00 | smoke: seeded watermark 2017-12-30T00:00:00+00:00 from max(fecha_de_cargue_en_el_secop) |
| 2026-05-16T02:34:48.511134+00:00 | `p6dx-8zbt` | smoke | ok | 1 | pass | 2026-05-15T00:00:00+00:00 | smoke: seeded watermark 2026-05-14T00:00:00+00:00 from max(fecha_de_publicacion_del) |
| 2026-05-16T02:34:51.841804+00:00 | `c82u-588k` | smoke | ok | 4947 | pass | 2026-05-04T12:17:08.880000+00:00 | smoke: seeded watermark 2026-05-03T12:17:08.880000+00:00 from max(fecha_actualizacion) |
| 2026-05-16T02:35:05.050379+00:00 | `rpmr-utcd` | smoke | ok | 14865 | pass | 2099-12-30T00:00:00+00:00 | smoke: seeded watermark 2026-04-20T00:00:00+00:00 from fallback |
| 2026-05-16T02:35:05.594357+00:00 | `wi7w-2nvm` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-04-21T00:00:00+00:00; no_new_rows |
| 2026-05-16T02:39:32.538175+00:00 | `rpmr-utcd` | smoke | ok | 14865 | pass | 2026-05-30T00:00:00+00:00 | smoke: seeded watermark 2026-04-20T00:00:00+00:00 from fallback |
| 2026-05-16T02:47:08.562824+00:00 | `rpmr-utcd` | smoke | ok | 14865 | pass | 2026-05-15T00:00:00+00:00 | smoke: seeded watermark 2026-04-20T00:00:00+00:00 from fallback |
| 2026-05-16T17:08:06.840145+00:00 | `5u9e-g5w9` | smoke | ok | 3323 | pass | 2022-12-06T00:00:00+00:00 | smoke: seeded watermark 2022-01-01T00:00:00+00:00 from fallback |
| 2026-05-16T17:08:08.794829+00:00 | `8tz7-h3eu` | smoke | ok | 1206 | pass | 2022-12-13T14:57:31.146000+00:00 | smoke: seeded watermark 2022-12-12T14:57:31.146000+00:00 from max(fecha_publicac_declarac) |
| 2026-05-16T17:08:09.409401+00:00 | `jbjy-vk9h` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-04T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:08:09.982586+00:00 | `qddk-cgux` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2017-12-31T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:08:10.487866+00:00 | `p6dx-8zbt` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-15T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:08:11.063570+00:00 | `c82u-588k` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-04T12:17:08.880000+00:00; no_new_rows |
| 2026-05-16T17:08:11.793517+00:00 | `rpmr-utcd` | smoke | failed | 0 | - | - | rpmr-utcd: zero rows have a parseable 'fecha_de_firma_del_contrato' — likely a column-name typo |
| 2026-05-16T17:08:12.346749+00:00 | `wi7w-2nvm` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-04-21T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:09:24.989155+00:00 | `5u9e-g5w9` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2022-12-06T00:00:00+00:00; only_future_watermarks |
| 2026-05-16T17:09:25.546407+00:00 | `8tz7-h3eu` | smoke | ok | 1 | pass | 2022-12-13T14:57:31.146000+00:00 | smoke: using existing watermark 2022-12-13T14:57:31.146000+00:00 |
| 2026-05-16T17:09:26.121831+00:00 | `jbjy-vk9h` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-04T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:09:26.824103+00:00 | `qddk-cgux` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2017-12-31T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:09:27.381463+00:00 | `p6dx-8zbt` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-15T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:09:28.001677+00:00 | `c82u-588k` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-04T12:17:08.880000+00:00; no_new_rows |
| 2026-05-16T17:09:28.726188+00:00 | `rpmr-utcd` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-15T00:00:00+00:00; only_future_watermarks |
| 2026-05-16T17:09:29.260489+00:00 | `wi7w-2nvm` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-04-21T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:10:17.800450+00:00 | `5u9e-g5w9` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2022-12-06T00:00:00+00:00; only_future_watermarks |
| 2026-05-16T17:10:18.431856+00:00 | `8tz7-h3eu` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2022-12-13T14:57:31.146000+00:00; no_new_rows |
| 2026-05-16T17:10:19.000541+00:00 | `jbjy-vk9h` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-04T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:10:19.518976+00:00 | `qddk-cgux` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2017-12-31T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:10:20.043993+00:00 | `p6dx-8zbt` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-15T00:00:00+00:00; no_new_rows |
| 2026-05-16T17:10:20.620269+00:00 | `c82u-588k` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-04T12:17:08.880000+00:00; no_new_rows |
| 2026-05-16T17:10:21.270795+00:00 | `rpmr-utcd` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-05-15T00:00:00+00:00; only_future_watermarks |
| 2026-05-16T17:10:21.913320+00:00 | `wi7w-2nvm` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-04-21T00:00:00+00:00; no_new_rows |
| 2026-05-21T04:41:57.301565+00:00 | `5u9e-g5w9` | full | ok | 24744 | pass | 2022-12-06T00:00:00+00:00 | - |
| 2026-05-21T04:43:03.385432+00:00 | `8tz7-h3eu` | full | ok | 328799 | pass | 2022-12-13T14:57:31.146000+00:00 | - |
| 2026-05-21T08:30:31.950006+00:00 | `jbjy-vk9h` | full | failed | 0 | - | - | 'fecha_de_firma' |
| 2026-05-21T11:34:58.779586+00:00 | `jbjy-vk9h` | full | ok | 5614448 | pass | 2026-05-04T00:00:00+00:00 | - |
| 2026-05-21T13:21:58.319543+00:00 | `qddk-cgux` | full | failed | 0 | - | - | exhausted retries for https://www.datos.gov.co/resource/qddk-cgux.json: The read operation timed out |
| 2026-05-21T14:50:23.392625+00:00 | `qddk-cgux` | full | failed | 0 | - | - | exhausted retries for https://www.datos.gov.co/resource/qddk-cgux.json: The read operation timed out |
| 2026-05-21T15:45:33.798810+00:00 | `qddk-cgux` | full | failed | 0 | - | - | Expecting value: line 8423 column 2 (char 28417403) |
| 2026-05-21T19:05:45.224771+00:00 | `qddk-cgux` | full | ok | 6122519 | pass | 2017-12-31T00:00:00+00:00 | - |
| 2026-05-21T20:31:48.056126+00:00 | `p6dx-8zbt` | full | ok | 8648158 | pass | 2026-05-21T00:00:00+00:00 | - |
| 2026-05-21T21:06:03.663571+00:00 | `c82u-588k` | full | ok | 9264493 | pass | 2026-05-04T12:17:08.880000+00:00 | - |
| 2026-05-21T23:18:07.703713+00:00 | `rpmr-utcd` | full | ok | 21869971 | pass | 2026-05-22T00:00:00+00:00 | - |
| 2026-05-22T04:57:52.936457+00:00 | `wi7w-2nvm` | full | ok | 42264321 | pass | 2026-04-21T00:00:00+00:00 | - |

## Operator evidence

### 2026-05-21/22 resilient full ingest

Command:

```bash
make ingest-phase7-full PHASE7_ARGS="--dataset rpmr-utcd --dataset wi7w-2nvm --min-free-gb 80 --timeout-seconds 120"
```

Results:

- `rpmr-utcd`: `21,869,971` rows, coverage `pass`, watermark `2026-05-22T00:00:00+00:00`.
- `wi7w-2nvm`: `42,264,321` rows, coverage `pass`, watermark `2026-04-21T00:00:00+00:00`.
- Phase 7 summary: `ok=2`, `skipped=0`, `failed=0`.

Observed resilience behavior:

- `rpmr-utcd` completed after warning that `1,729,586` rows landed in the `year=0/month=0` sentinel partition due to unparseable `fecha_de_firma_del_contrato`, and `110` future-dated rows landed in the sentinel partition without advancing the watermark.
- Keyset pagination was used for the large full-refresh datasets. `wi7w-2nvm` drained dense same-date buckets with `fecha_de_registro = <date> AND :id > <last_id>`, then returned to `fecha_de_registro > <date>` handoffs.
- Retry handling recovered from Socrata 500/read-timeout responses during the full sequence; observed recoveries included `2024-11-28`, `2024-12-10`, `2025-06-05`, `2025-08-05`, `2025-08-11`, and `2025-08-19`.
- Resource snapshots during `wi7w-2nvm` stayed bounded: disk remained about `212 GiB` free, staging grew from about `627 MiB` to about `961 MiB`, and the ingest process stayed roughly `253,200` to `390,672` KB RSS. Final staging was `0B`.

Lake output checks after completion:

- `lake/raw/source=qddk-cgux`: `1.4G`, `1,173` parquet files.
- `lake/raw/source=p6dx-8zbt`: `1.3G`, `1,453` parquet files.
- `lake/raw/source=c82u-588k`: `613M`, `1,052` parquet files.
- `lake/raw/source=rpmr-utcd`: `3.2G`, `3,481` parquet files.
- `lake/raw/source=wi7w-2nvm`: `1.0G`, `4,646` parquet files.

## 2026-05-21/22 full ingest operator notes

Command:

```bash
make ingest-phase7-full PHASE7_ARGS="--dataset rpmr-utcd --dataset wi7w-2nvm --min-free-gb 80 --timeout-seconds 120"
```

The resumed Phase 7 full ingest completed the remaining large sources with keyset pagination enabled for the allowlisted datasets. `rpmr-utcd` finished at `2026-05-21T23:18:07.703713+00:00` with 21,869,971 rows across 314 partitions and watermark `2026-05-22T00:00:00+00:00`. `wi7w-2nvm` finished at `2026-05-22T04:57:52.936457+00:00` with 42,264,321 rows and watermark `2026-04-21T00:00:00+00:00`.

Recovered transient source failures:

- `qddk-cgux` had prior failed full attempts on 2026-05-21 from Socrata read timeouts and one malformed JSON response, then completed successfully at `2026-05-21T19:05:45.224771+00:00` with 6,122,519 rows.
- `wi7w-2nvm` recovered from transient Socrata 500/read-timeout events during the long keyset run. Notable recovered retries included handoffs after `2024-11-28`, `2024-12-10`, and a dense `2025-06-05` same-date cursor page.

Data quality routing:

- `rpmr-utcd`: 1,729,586 of 21,869,971 rows had unparseable `fecha_de_firma_del_contrato` and landed in the `year=0/month=0` sentinel partition.
- `rpmr-utcd`: 110 rows had future `fecha_de_firma_del_contrato` values after the ingest run's `now` and landed in the sentinel partition without advancing the watermark.

Resource evidence:

- During `wi7w-2nvm`, resident memory stayed roughly in the 240-432 MB range while staging grew to about 1.0 GB before promotion.
- Free disk stayed above 212 GiB, well above the `--min-free-gb 80` guard.
- After promotion, `lake/meta/ingest_staging` returned to `0B`, `lake/raw` measured 8.7 GB, and source parquet counts were: `qddk-cgux` 1,173 files, `p6dx-8zbt` 1,453 files, `c82u-588k` 1,052 files, `rpmr-utcd` 3,481 files, `wi7w-2nvm` 4,646 files.
| 2026-06-05T01:01:20.098255+00:00 | `u8cx-r425` | smoke | ok | 63000 | pass | 2026-06-03T00:00:00+00:00 | smoke: seeded watermark 2026-06-02T00:00:00+00:00 from max(fecha_de_carga) |
