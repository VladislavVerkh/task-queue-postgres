# Grafana dashboards in this folder

`*.json` dashboard files are loaded by Grafana provisioning as strict JSON.
JSON format does not allow inline comments, so parameter explanations are documented here.

## Common top-level fields

- `annotations`: Built-in annotation settings (events/alerts overlay).
- `editable`: Whether dashboard can be edited in UI.
- `fiscalYearStartMonth`: Fiscal calendar start month for time analytics.
- `graphTooltip`: Tooltip mode across panels.
- `id`: Internal Grafana DB id (`null` for provisioned dashboards).
- `links`: Dashboard-level external/internal links.
- `panels`: Panel definitions (queries + visualization options).
- `refresh`: Auto-refresh interval.
- `schemaVersion`: Grafana dashboard schema version.
- `style`: Legacy theme hint.
- `tags`: Dashboard tags used for search/filter.
- `templating`: Variables and dropdown filters.
- `time`: Default time range when opening dashboard.
- `timepicker`: Allowed quick ranges/refresh options.
- `timezone`: Timezone mode (`browser`, `utc`, etc.).
- `title`: Dashboard title shown in UI.
- `uid`: Stable unique identifier used in URLs/provisioning.
- `version`: Dashboard revision number.

## Common panel fields

- `datasource`: Source backend (`prometheus`, `loki`, `postgres`).
- `fieldConfig.defaults`: Default visualization/format/threshold settings.
- `fieldConfig.overrides`: Per-series overrides.
- `gridPos`: Panel position and size (`h`, `w`, `x`, `y`).
- `id`: Unique panel id inside dashboard.
- `options`: Visualization-specific options (legend, tooltip, reduce, etc.).
- `targets`: Query list (`expr`/SQL/log query, `refId`, legend labels).
- `title`: Panel title.
- `type`: Panel visualization type (`timeseries`, `stat`, `table`, etc.).

## File-specific purpose

- `task-queue-overview.json`: Task queue runtime metrics (workers/throughput/errors).
- `java-overview.json`: JVM and application runtime metrics.
- `postgres-overview.json`: PostgreSQL health/performance metrics.
