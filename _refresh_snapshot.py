from bot_new import WialonClient, _rebuild_unit_snapshot, load_pipeline_config

cfg = load_pipeline_config()
token = cfg.wialon_token or (cfg.wialon_extra_tokens[0] if cfg.wialon_extra_tokens else None)
if not token:
    raise SystemExit("Нет Wialon токена для pipeline.")

client = WialonClient(cfg.wialon_host, token)
try:
    snapshot, total, duration = _rebuild_unit_snapshot(client)
    print(f"Загружено {len(snapshot)} объектов (API элементов: {total}) за {duration:.1f} c")
finally:
    client.close()
