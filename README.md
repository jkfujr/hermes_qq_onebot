# hermes-qq-onebot

QQ OneBot v11 平台适配器插件，为 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 添加 QQ 支持。

基于 NapCatQQ / go-cqhttp / Lagrange.OneBot / LLOneBot 等 OneBot v11 兼容实现。

## 架构

```
QQ 客户端 ←→ OneBot 实现 (NapCat/go-cqhttp)
                  ↓ WebSocket (事件) + HTTP (API，可选)
             QQ 适配器插件 (hermes-qq-onebot)
```

- **WebSocket**：必须，用于接收事件和发送 API 调用
- **HTTP API**：可选但推荐开启，便于调试（curl 即可调用）、长程任务不受 WS 超时限制、`get_file` 等操作更稳定

## 支持的功能

- 私聊 / 群聊消息收发
- @提及检测
- 关键词触发（`mention_patterns`，群聊中匹配关键词即回复，类似 Telegram）
- 图片、语音、文件收发
- 回复消息
- emoji 表情回应（群聊）和戳一戳（私聊）（可通过 `enable_reaction: false` 关闭）
- 长消息自动拆分 + 合并转发（群聊，避免刷屏）
- 正向 WebSocket (Hermes → LLBot) + 反向 WebSocket (LLBot → Hermes)
- 群白名单 / 黑名单（按群号过滤，白名单优先）
- 用户授权（通过网关层 `QQ_ONEBOT_ALLOWED_USERS` 环境变量统一管理）
- HTTP API 独立通道 (可与 WS 并用)
- 消息去重

## 安装

```bash
hermes plugins install chrysoljq/hermes-qq-onebot --enable
```

安装完成后，插件会提示输入 `QQ_ONEBOT_WS_URL` 环境变量（可选，留空使用 config.yaml 配置）。

## 配置

在 `~/.hermes/config.yaml` 中添加：

```yaml
platforms:
  qqonebot:
    enabled: true
    extra:
      # OneBot HTTP API 地址（可选，推荐开启）
      http_api_url: "http://127.0.0.1:5700"
      # WebSocket 反向模式（推荐，adapter 起 server 等 OneBot 连上来）
      reverse_mode: true
      reverse_host: "0.0.0.0"
      reverse_port: 6700
      # 正向模式（adapter 主动连 OneBot）
      # ws_host: "127.0.0.1"
      # ws_port: 3001
      # ws_path: "/onebot/v11/ws"
      access_token: ""
      show_qq_id: false         # 在 user_name 里附带 QQ 号，如 用户名(123456)
      # 群白名单（逗号分隔群号），仅响应这些群的消息（优先于黑名单）
      group_whitelist: ""
      # 群黑名单（逗号分隔群号），忽略这些群的消息（白名单为空时生效）
      group_blacklist: ""
      # 群聊关键词触发（正则，不区分大小写），匹配到即触发回复（不需要 @）
      # 支持字符串或列表，也可用环境变量 QQ_MENTION_PATTERNS=芙芙,帮我
      # mention_patterns:
      #   - "芙芙"
      #   - "帮我"
      # 关闭 emoji 表情回应和戳一戳（默认 true）
      # enable_reaction: false
```

## 环境变量（可选）

```bash
QQ_ONEBOT_WS_URL=ws://127.0.0.1:3001/onebot/v11/ws  # 正向模式
QQ_ONEBOT_ALLOWED_USERS=123456,789012                 # 允许的 QQ 号
QQ_ONEBOT_ALLOW_ALL_USERS=false                       # 允许所有用户
QQ_HOME_CHANNEL=qq_group_123456789                    # 默认发送目标
QQ_ONEBOT_GROUP_WHITELIST=111111,222222                # 群白名单（仅响应这些群）
QQ_ONEBOT_GROUP_BLACKLIST=333333,444444                # 群黑名单（忽略这些群）
```

## 卸载

```bash
hermes plugins remove qqonebot
```

## 文件说明

```
hermes-qq-onebot/
├── qqonebot.py            # QQ 适配器主文件
├── plugin.yaml            # 插件声明
├── __init__.py            # 导出 register
├── adapter.py             # 注册到 platform_registry
├── README.md
└── ...
```

## 手动安装（不推荐）

如果无法使用 `hermes plugins install`，可以手动安装：

```bash
# 1. 克隆仓库
git clone https://github.com/chrysoljq/hermes-qq-onebot.git
cd hermes-qq-onebot

# 2. 复制到 hermes plugins 目录
mkdir -p ~/.hermes/plugins/qqonebot
cp qqonebot.py plugin.yaml __init__.py adapter.py ~/.hermes/plugins/qqonebot/

# 3. 启用插件
hermes plugins enable qqonebot

# 4. 安装依赖
pip install websockets

# 5. 重启 gateway
hermes gateway restart
```
