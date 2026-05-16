# Phase 7 ingest log

| finished_at_utc | dataset | mode | status | rows | coverage | watermark | note |
|---|---|---|---:|---:|---|---|---|
| 2026-05-16T02:34:43.560517+00:00 | `2jzx-383z` | smoke | failed | 0 | - | - | exhausted retries for https://www.datos.gov.co/resource/2jzx-383z.json: Client error '403 Forbidden' for url 'https://www.datos.gov.co/resource/2jzx-383z.json?%24select=max%28fecha_de_vinculaci_n%29' For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403 |
| 2026-05-16T02:34:46.244396+00:00 | `jbjy-vk9h` | smoke | ok | 842 | pass | 2026-05-04T00:00:00+00:00 | smoke: seeded watermark 2026-05-03T00:00:00+00:00 from max(fecha_de_firma) |
| 2026-05-16T02:34:47.561504+00:00 | `qddk-cgux` | smoke | ok | 875 | pass | 2017-12-31T00:00:00+00:00 | smoke: seeded watermark 2017-12-30T00:00:00+00:00 from max(fecha_de_cargue_en_el_secop) |
| 2026-05-16T02:34:48.511134+00:00 | `p6dx-8zbt` | smoke | ok | 1 | pass | 2026-05-15T00:00:00+00:00 | smoke: seeded watermark 2026-05-14T00:00:00+00:00 from max(fecha_de_publicacion_del) |
| 2026-05-16T02:34:51.841804+00:00 | `c82u-588k` | smoke | ok | 4947 | pass | 2026-05-04T12:17:08.880000+00:00 | smoke: seeded watermark 2026-05-03T12:17:08.880000+00:00 from max(fecha_actualizacion) |
| 2026-05-16T02:35:05.050379+00:00 | `rpmr-utcd` | smoke | ok | 14865 | pass | 2099-12-30T00:00:00+00:00 | smoke: seeded watermark 2026-04-20T00:00:00+00:00 from fallback |
| 2026-05-16T02:35:05.594357+00:00 | `wi7w-2nvm` | smoke | skipped | 0 | - | - | smoke: using existing watermark 2026-04-21T00:00:00+00:00; no_new_rows |
| 2026-05-16T02:39:32.538175+00:00 | `rpmr-utcd` | smoke | ok | 14865 | pass | 2026-05-30T00:00:00+00:00 | smoke: seeded watermark 2026-04-20T00:00:00+00:00 from fallback |
| 2026-05-16T02:47:08.562824+00:00 | `rpmr-utcd` | smoke | ok | 14865 | pass | 2026-05-15T00:00:00+00:00 | smoke: seeded watermark 2026-04-20T00:00:00+00:00 from fallback |
