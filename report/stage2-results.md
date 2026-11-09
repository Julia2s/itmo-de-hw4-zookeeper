# Этап 2. Ансамбль ZooKeeper

Проверка выполнена на трёх контейнерах ZooKeeper 3.9.6 из `compose.yaml`. Ниже приведены команды и фактические существенные строки вывода. Полные журналы сохранены в `logs/zk/`.

## Исходное состояние и роли

Команды:

```powershell
docker compose ps
docker compose exec zk1 zkServer.sh status
docker compose exec zk2 zkServer.sh status
docker compose exec zk3 zkServer.sh status
```

Результат: все три контейнера были `Up`; роли — `zk1: follower`, `zk2: follower`, `zk3: leader`.

## Отказ одного сервера

Команды:

```powershell
docker compose stop zk2
docker compose ps
docker compose exec zk1 zkServer.sh status
docker compose exec zk3 zkServer.sh status
$commands = @(
  'create /one-node-check-20261109 one-server-down-ok',
  'get /one-node-check-20261109',
  'delete /one-node-check-20261109',
  'quit'
)
$commands | docker compose exec -T zk1 timeout 20s zkCli.sh -server localhost:2181
```

Результат: `zk2` остановлен, `zk1` и `zk3` оставались запущены; `zk3` показывал `Mode: leader`, `zk1` — `Mode: follower`. Запись через оставшийся ансамбль прошла:

```text
Created /one-node-check-20261109
one-server-down-ok
```

Тестовый узел удалён командой `delete`.

## Потеря кворума при остановке двух серверов

Команды:

```powershell
docker compose stop zk2 zk3
docker compose ps
$commands = @('create /quorum-check-20261109 test', 'quit')
$commands | docker compose exec -T zk1 timeout 35s zkCli.sh -server localhost:2181
```

`docker compose ps` показал только `zk1` со статусом `Up`; `zk2` и `zk3` были остановлены. Запись завершилась ошибкой:

```text
KeeperErrorCode = ConnectionLoss for /quorum-check-20261109
Session 0x0 ... Closing socket connection.
Unable to read additional data from server sessionid 0x0, likely server has closed socket
Код завершения проверки: 1
```

Узел `/quorum-check-20261109` не был создан.

## Восстановление ансамбля

Команды:

```powershell
docker compose start zk2 zk3
docker compose ps
docker compose exec zk1 zkServer.sh status
docker compose exec zk2 zkServer.sh status
docker compose exec zk3 zkServer.sh status
```

Результат: все три контейнера снова `Up`; статусы после восстановления: `zk1: follower`, `zk2: follower`, `zk3: leader`.

## Эфемерный узел

В первой клиентской сессии узел создан и прочитан, затем сессия закрыта командой `quit`:

```powershell
$commands = @(
  'create -e /hw4-ephemeral-20261109 ephemeral-value',
  'get /hw4-ephemeral-20261109',
  'quit'
)
$commands | docker compose exec -T zk1 timeout 20s zkCli.sh -server localhost:2181
```

```text
Created /hw4-ephemeral-20261109
ephemeral-value
WatchedEvent state:Closed
Session: ... closed
```

Во второй клиентской сессии:

```powershell
$commands = @('get /hw4-ephemeral-20261109', 'quit')
$commands | docker compose exec -T zk1 timeout 20s zkCli.sh -server localhost:2181
```

```text
Node does not exist: /hw4-ephemeral-20261109
```

## Журналы

- Исходные роли и потеря кворума: [`08_two_nodes_no_quorum.txt`](../logs/zk/08_two_nodes_no_quorum.txt)
- Один сервер остановлен, запись успешна: [`08_one_node_down_ensemble_works.txt`](../logs/zk/08_one_node_down_ensemble_works.txt)
- Роли после восстановления двух серверов: [`09_two_nodes_restored.txt`](../logs/zk/09_two_nodes_restored.txt)
- Все роли после восстановления одиночного отказа: [`10_after_one_server_restored.txt`](../logs/zk/10_after_one_server_restored.txt)
- Создание и чтение эфемерного узла: [`11_ephemeral_node_session.txt`](../logs/zk/11_ephemeral_node_session.txt)
- Проверка исчезновения узла: [`12_ephemeral_node_absent.txt`](../logs/zk/12_ephemeral_node_absent.txt)
