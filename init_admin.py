from pipeline.services.admin_storage import get_admin_storage
from pipeline.services.passwords import hash_password


def init() -> None:
    storage = get_admin_storage()

    # 1. Создаем корневой узел (если нет)
    nodes = storage.list_nodes()
    if not nodes:
        print("Создаю корневой узел 'Main'...")
        root_node = storage.create_node(name="Main", parent_id=None, order=0)
    else:
        root_node = nodes[0]
        print(f"Корневой узел уже есть: {root_node.name} (id={root_node.id})")

    # 2. Создаем админа (если нет)
    login = "admin"
    password = "admin"  # Не забудь сменить в проде

    user = storage.get_user_by_login(login)
    if not user:
        print(f"Создаю пользователя '{login}'...")
        storage.create_user(
            login=login,
            password_hash=hash_password(password),
            display_name="Super Admin",
            node_id=root_node.id,
            is_admin=True,
            can_manage_users=True,
            can_manage_units=True,
        )
        print(f"✅ Пользователь '{login}' создан. Пароль: '{password}'")
    else:
        print(f"✅ Пользователь '{login}' уже существует (id={user.id})")


if __name__ == "__main__":
    init()
