import time

from pipeline.events import Event
from pipeline.services.storage_service import get_pipeline_storage_service
from pipeline.engine.status import compute_status


def test_flow() -> None:
    print("--- Start Data Flow Test ---")
    storage = get_pipeline_storage_service()

    # 1. Создаем тестовое событие
    now = int(time.time())
    test_unit_id = 999999  # Используем несуществующий ID
    ev = Event(
        unit_id=test_unit_id,
        device_ts=now,
        received_ts=now,
        latitude=55.75,
        longitude=37.61,
        speed=15.0,  # Движение
        course=180,
        params={"ign": 1, "pwr_ext": 12.5},
        source="sanity_check",
    )

    print(f"1. Генерируем событие для unit_id={test_unit_id}...")

    # 2. Пишем в базу (и архив)
    count = storage.store_events([ev])
    if count == 1:
        print("✅ store_events вернул 1 (успешная запись)")
    else:
        print(f"❌ store_events вернул {count} (ожидалось 1)")

    # 3. Читаем из базы (fetch_period)
    print("2. Читаем обратно из DB (fetch_period)...")
    read_back = storage.fetch_period(test_unit_id, now - 10, now + 10)
    if len(read_back) >= 1:
        print(f"✅ Событие найдено в БД. Скорость: {read_back[0].speed}")
    else:
        print("❌ Событие НЕ найдено в БД!")

    # 4. Проверяем Latest Metrics (кэш)
    print("3. Проверяем LatestTelemetryStore...")
    latest = storage.get_latest_metrics(test_unit_id)
    if latest and latest.get("lat") == 55.75:
        print("✅ Latest Metrics обновлены корректно")
    else:
        print(f"❌ Latest Metrics пустые или неверные: {latest}")

    # 5. Проверяем статус (engine)
    print("4. Расчет статуса (compute_status)...")
    snapshot = {"t": now, "lat": 55.0, "lon": 37.0}
    status = compute_status(
        snapshot=snapshot,
        latest=latest or {},
        unit_config={"advanced": {}},
        unit_id=test_unit_id,
        now=now,
        health_ok=True,
    )

    print(
        f"   Результат: Online={status['online']}, Status={status['status']}, "
        f"Ign={status['ignition']}"
    )

    if status["online"] and status["status"] == "moving" and status["ignition"]:
        print("✅ Статус рассчитан верно (Moving, Online, Ign=True)")
    else:
        print("❌ Ошибка в расчете статуса")

    print("\n🎉 SANITY CHECK завершён (см. сообщения выше).")


if __name__ == "__main__":
    test_flow()
