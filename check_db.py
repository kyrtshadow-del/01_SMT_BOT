import os
import psycopg
from pipeline.config.defaults import load_from_env


def check() -> None:
    print("--- 1. Проверка подключения ---")
    # Загружаем конфиг, чтобы убедиться, что PIPELINE_STORAGE_ROOT и др. не ломают импорт
    _ = load_from_env()
    dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
    print(f"DSN: {dsn}")

    if not dsn:
        print("❌ ОШИБКА: Нет DSN в переменных окружения!")
        return

    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            print("✅ Подключение к Postgres успешно")

            with conn.cursor() as cur:
                print("\n--- 2. Проверка таблиц ---")
                tables = ["events", "nodes", "users", "units_meta", "units", "devices"]
                missing = []
                for t in tables:
                    cur.execute("SELECT to_regclass(%s)", (t,))
                    if cur.fetchone()[0] is None:
                        missing.append(t)
                    else:
                        print(f"✅ Таблица '{t}' существует")

                if missing:
                    print(f"❌ ОТСУТСТВУЮТ ТАБЛИЦЫ: {missing}")
                    print("💡 Совет: Если нет 'events', накати scripts/sql/001_units.sql или schema_pg.sql")
                    print("💡 Совет: Если нет 'users/nodes', достаточно один раз инициализировать AdminStorage.")
                else:
                    print("\n🎉 Все таблицы на месте!")

                print("\n--- 3. Проверка индексов events ---")
                cur.execute("SELECT indexname FROM pg_indexes WHERE tablename = 'events'")
                indexes = [row[0] for row in cur.fetchall()]
                print(f"Найдены индексы: {indexes}")
                has_idx = any("unit" in i and "ts" in i for i in indexes)
                if has_idx:
                    print("✅ Есть индекс по unit/ts (или похожий) — запросы треков будут быстрые")
                else:
                    print("⚠️ ВНИМАНИЕ: Не видно индекса по unit_id/device_ts — стоит добавить для больших объёмов")

    except Exception as e:  # pragma: no cover - диагностический скрипт
        print(f"❌ ОШИБКА ПОДКЛЮЧЕНИЯ: {e}")


if __name__ == "__main__":
    check()
