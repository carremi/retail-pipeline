# Programación del pipeline (macOS / launchd)

El pipeline corre todos los días a las **04:00 local** vía `launchd`.

## Archivos

- `scripts/run_daily.sh` — wrapper que activa venv, asegura Docker y lanza el pipeline.
- `~/Library/LaunchAgents/com.retailpipeline.daily.plist` — job de launchd.
- `logs/launchd.out.log` y `logs/launchd.err.log` — stdout/stderr de cada corrida.

## Operación

```bash
# Cargar el job (se programa automáticamente)
launchctl load   ~/Library/LaunchAgents/com.retailpipeline.daily.plist

# Descargar
launchctl unload ~/Library/LaunchAgents/com.retailpipeline.daily.plist

# Forzar una corrida ahora
launchctl start  com.retailpipeline.daily

# Ver estado
launchctl list | grep retailpipeline
```

## Ver historia y logs

```bash
# Historia en BD
docker compose exec postgres psql -U retail_user -d retail_pipeline \
  -c "SELECT run_id, started_at, status FROM ops.pipeline_runs ORDER BY run_id DESC LIMIT 10;"

# Logs en disco
tail -f logs/pipeline.log
tail -f logs/launchd.out.log
```

## Troubleshooting

- **`launchctl list` no lista el job** → revisa que el plist esté en `~/Library/LaunchAgents/` y bien formado: `plutil -lint ~/Library/LaunchAgents/com.retailpipeline.daily.plist`
- **El job aparece pero exit code != 0** → revisa `logs/launchd.err.log`. Lo más común: Docker no estaba corriendo.
- **Pipeline corrió pero no hay datos nuevos** → revisa `ops.pipeline_runs.status` y `sources_summary`. Si todos los extractores devolvieron 0, los simuladores no generaron archivos nuevos (en producción real, no hay nada que ingerir hasta el día siguiente).
