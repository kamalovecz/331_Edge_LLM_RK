# Edge-LLM-Infra — 分布式边缘 LLM 推理框架 架构分析

> **项目路径**: `/home/orangepi/work/Edge-LLM-Infra-master/`  
> **作者**: M5Stack Technology CO LTD  
> **许可证**: MIT  
> **分析日期**: 2026-06-30

---

## 一、一句话总结

这是一个**分布式 LLM 推理微服务框架**。它把每类 AI 能力（LLM、ASR、TTS、CV…）封装成可动态注册/销毁的微服务单元（Unit），通过 **ZMQ 消息总线 + TCP 桥接** 实现单元间的异步通信和生命周期管理。

---

## 二、五层架构

```
┌─────────────────────────────────────────────────────┐
│                  用户 / 外部系统                      │
│              HTTP / WebSocket / TCP                  │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│  ⑤ 接入层 (session.h / tcp_comm.cpp)                 │
│     TcpServer + TcpSession: 每连接创建一个ZMQ桥接      │
│     外部请求 → 解析 JSON → 路由到 unit_action_match()  │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│  ④ 编排层 (infra-controller)                         │
│     StackFlow: 事件驱动的推理单元基类                  │
│     llm_channel_obj: 每个推理任务的 ZMQ 通道管理       │
│     RPC Actions: setup / pause / exit / taskinfo     │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│  ③ 管理层 (unit-manager / remote_server)             │
│     sys_rpc_server: 全局 IPC 服务 (allocate/release)  │
│     unit_data: 推理单元元数据 (port/work_id/zmq_url)   │
│     key_sql: 线程安全的全局键值存储 (spinlock)         │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│  ② 通信层 (hybrid-comm)                              │
│     pzmq: 6种模式的 ZMQ 高级封装                       │
│     pzmq_data: 多段消息的解析与构造                     │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│  ① 网络层 (network/)                                  │
│     muduo 风格 Reactor: EventLoop → Poller → Channel  │
│     TcpServer / TcpClient / TcpConnection            │
└─────────────────────────────────────────────────────┘
```

---

## 三、层间调用关系（核心流程）

### 3.1 启动流程

```
main()
  ├─ load_default_config()         ← 读 master_config.json 到 key_sql
  ├─ remote_server_work()          ← 注册 sys RPC 服务
  │   ├─ pzmq("sys")                → IPC socket: /tmp/rpc.sys
  │   ├─ register "register_unit"   → rpc_allocate_unit
  │   ├─ register "release_unit"    → rpc_release_unit
  │   ├─ register "sql_select/set"  → rpc_sql_select/set
  │   └─ 事件循环线程启动            → zmq_event_loop (REP模式)
  │
  ├─ tcp_work()                    ← 启动 TCP 桥接服务器
  │   ├─ TcpServer(port=config)     → 监听外部连接
  │   ├─ onConnection               → 创建 TcpSession + zmq_bus_com
  │   ├─ onMessage                  → 解析 JSON → unit_action_match()
  │   └─ loop.loop()                → 主线程进入 Reactor 事件循环
  │
  └─ while(!exit) sleep(1)          ← 主线程挂起等待信号
```

### 3.2 推理请求处理全链路

```
外部客户端(TCP)
  │ 发送 JSON: {"request_id":"xxx", "work_id":"llm.0",
  │             "action":"inference", "object":"...", "data":"..."}
  ▼
① TcpSession::on_data(msg)
  │
  ▼
② unit_action_match(com_id, json_str)
  │  解析 work_id → 提取 unit_name ("llm") + index (0)
  │  判断 action == "inference"
  │
  ├─ zmq_bus_publisher_push("llm.0", raw_data)
  │   │ SAFE_READING → 从 key_sql 获取 unit_data*
  │   │ unit_data::send_msg(json_str)
  │   │
  │   ▼
  │  pzmq→PUB→ ipc:///tmp/llm.0.inference_url.socket
  │   │
  │   ▼
  │  llm_channel_obj::subscriber (ZMQ_SUB 接收)
  │   │ subscriber_event_call() → 提取 object/data
  │   │
  │   ▼
  │  task_user_data(object, data)
  │   │ 可选 stream 解码 (decode_stream)
  │   │
  │   ▼
  │  llm_task::inference(data)
  │   │ 实际模型推理...
  │   │
  │   ▼
  │  task_output(data, finish)
  │   │ 构造响应 JSON
  │   │
  │   ▼
  │  llm_channel_obj::send() → pzmq::send_data()
  │   │
  │   ├─→ PUB → output_url (回推结果给调用方)
  │   └─→ PUSH → usr_url (用户自定义回调)
  │
  └─ 如果 action != "inference"
      └─ remote_call(com_id, json_str)
          │ pzmq(work_id).call_rpc_action(action, data)
          │
          ▼
        目标 Unit 的 rpc_ctx_ (ZMQ_REP)
          │ 回调注册的 RPC 函数 (setup/pause/exit/taskinfo)
          │
          ▼
        事件入队 event_queue_.enqueue(EVENT_XXX, data)
          │
          ▼
        even_loop 线程处理 → 调用虚函数 setup()/exit()/pause()
```

### 3.3 推理单元生命周期

```
外部调用 setup()
  │
  ▼
remote_server: rpc_allocate_unit(unit_name)
  ├─ 分配 work_id: "llm.N" (N = 自增计数器)
  ├─ 分配 output_url:   ipc:///tmp/llm.N.output_url.socket (bitmap 端口分配)
  ├─ 分配 inference_url: ipc:///tmp/llm.N.inference_url.socket
  ├─ 创建 unit_data → SAFE_SETTING(work_id, unit_data*)
  └─ 返回 (port, output_url, inference_url) 给调用方

StackFlow::setup():
  ├─ 创建 llm_channel_obj (持有 PUB→inference_url, PUSH→output_url)
  ├─ 调用虚函数 setup(work_id, object, data) → 子类加载模型
  ├─ subscriber_work_id() → ZMQ_SUB 订阅推理请求
  └─ Unit 进入 RUNNING 状态

外部调用 exit()
  │
  ▼
StackFlow::exit():
  ├─ 调用虚函数 exit(work_id, object, data) → 子类卸载模型
  ├─ stop_subscriber() → 取消 ZMQ 订阅
  ├─ sys_release_unit(work_id) → 回收端口/释放 unit_data
  └─ Unit 回到 IDLE 状态
```

---

## 四、各模块职责与配合关系

### 4.1 hybrid-comm — ZMQ 通信原语

| 类 | 职责 | 对等角色 |
|---|---|---|
| `pzmq` | ZMQ 6 模式统一封装 | — |
| `pzmq_data` | 多段 ZMQ 消息的构造与解析 | — |

**6 种通信模式与使用场景**：

```
PUB/SUB   → 一对多广播    (推理结果推送给所有订阅者)
PUSH/PULL → 负载均衡分发  (推理请求分发给空闲 Worker)
REQ/REP   → RPC 同步调用  (setup/exit 等控制指令)
```

关键设计：
- 默认使用 **Unix Domain Socket** (`ipc:///tmp/rpc.xxx`)，性能远高于 TCP loopback
- `pzmq` 构造为惰性连接：RPC Client 只在首次 `call_rpc_action()` 时才创建 ZMQ socket
- RPC Server 模式使用独立线程 `zmq_event_loop` 轮询 ZMQ socket
- 内置 `list_action` RPC：任何注册过的 Unit 都可以被查询其支持的操作列表
- 支持动态注册/注销 RPC action（`register_rpc_action` / `unregister_rpc_action`）

**pzmq 内部 socket 生命周期**：

```
构造 pzmq("server_name")     → 仅存储 server 名，不创建 socket
register_rpc_action("act")   → 首次注册时创建 REP socket + 事件循环线程
call_rpc_action("act", data) → 创建 REQ socket → 发送 → 接收 → 销毁 socket
析构                          → 关闭 socket + join 线程 + 删除 IPC 文件
```

### 4.2 network/ — TCP 网络库

经典的 **muduo 风格 Reactor 模型**：

```
EventLoop (one loop per thread)
  ├─ Poller (epoll 封装)
  ├─ Channel (fd + events 抽象)
  │
  ├─ Acceptor   → 监听端口，accept 新连接
  ├─ Connector  → 主动发起连接
  ├─ TcpConnection → 已建立连接 (带 Buffer)
  │
  ├─ TcpServer  → Acceptor + EventLoopThreadPool
  └─ TcpClient  → Connector + EventLoop
```

### 4.3 unit-manager — 全局资源管理器

**`remote_server` 注册的 RPC 服务**：

| RPC Action | 功能 | 实现函数 |
|---|---|---|
| `register_unit` | 分配 Unit: port/work_id/ZMQ URL | `rpc_allocate_unit` |
| `release_unit` | 回收 Unit: 释放端口/ZMQ/内存 | `rpc_release_unit` |
| `sql_select` | 读全局 key-value | `rpc_sql_select` |
| `sql_set` | 写全局 key-value | `rpc_sql_set` |
| `sql_unset` | 删全局 key-value | `rpc_sql_unset` |

**`key_sql`** — 线程安全的全局状态存储：

```cpp
std::unordered_map<std::string, std::any> key_sql;
pthread_spinlock_t key_sql_lock;

// 线程安全读
#define SAFE_READING(_val, _type, _key)
    pthread_spin_lock(&key_sql_lock);
    _val = std::any_cast<_type>(key_sql.at(_key));
    pthread_spin_unlock(&key_sql_lock);

// 线程安全写
#define SAFE_SETTING(_key, _val)
    pthread_spin_lock(&key_sql_lock);
    key_sql[_key] = _val;
    pthread_spin_unlock(&key_sql_lock);
```

**`unit_data`** — 推理单元元数据：

```cpp
class unit_data {
    string work_id;        // "llm.0", "asr.3"
    string output_url;     // ipc:///tmp/llm.0.output_url.socket
    string inference_url;  // ipc:///tmp/llm.0.inference_url.socket
    int port_;             // ZMQ 端口编号
    pzmq user_inference_chennal_;  // PUB socket → 向该 Unit 推送推理请求
};
```

**端口分配策略**：从配置的 `[config_zmq_min_port, config_zmq_max_port]` 范围内用 **bitmap** (`std::vector<bool> port_list`) 高效分配/回收，避免端口冲突。

### 4.4 infra-controller — 推理单元基类

**`StackFlow`** 是所有推理服务的基类：

```
StackFlow 生命周期状态机:

  ┌─────────┐   setup()   ┌──────────┐   exit()   ┌─────────┐
  │  IDLE    │ ──────────→ │ RUNNING  │ ────────→ │  IDLE    │
  └─────────┘             └──────────┘           └─────────┘
       ↑                       │
       └─── pause() ───────────┘

事件驱动模型:

  外部 RPC 调用
      │
      ▼
  event_queue_.enqueue(EVENT_XXX, data)
      │
      ▼
  even_loop 线程 (独立线程, 阻塞等待事件)
      │
      ▼
  event_queue_.process()
      │
      ├─ EVENT_SETUP    → _setup()   → 虚函数 setup(work_id, object, data)
      ├─ EVENT_EXIT     → _exit()    → 虚函数 exit(work_id, object, data)
      ├─ EVENT_PAUSE    → _pause()   → 虚函数 pause(work_id, object, data)
      └─ EVENT_TASKINFO → _taskinfo() → 虚函数 taskinfo(work_id, object, data)
```

- 使用 **eventpp** 事件队列解耦 RPC 线程和业务线程
- RPC 调用只负责入队 → 立即返回 `"None"`
- `even_loop` 线程串行处理业务逻辑 → 避免并发问题
- `status_` 原子变量控制：为 0 时拒绝处理（setup 未完成），为 1 时正常工作

**`llm_channel_obj`** — 推理任务的通信管理器：

```
llm_channel_obj 内部 ZMQ Socket 管理:

  zmq_[-1]  = PUB socket   → 向 inference_url 推送推理请求
  zmq_[-2]  = PUSH socket  → 向 output_url 推送结果给调用方
  zmq_[id]  = SUB socket   → 订阅特定 work_id 的输出 (按需创建, id≥0)

索引规则:
  负数  → 基础设施 socket (生命周期与 channel 相同)
  正数  → 动态订阅 socket (按 work_id 创建/销毁)
```

**`send()` 消息路由**：

```cpp
int send(object, data, error_msg, work_id) {
    // 1. 构造 JSON: {request_id, work_id, created, object, data, error}
    // 2. send_raw_to_pub(out)  → 推送到 PUB (广播给所有订阅者)
    // 3. 如果 enoutput_=true   → send_raw_to_usr(out) → PUSH (单播给调用方)
}
```

### 4.5 node/test — 业务实现示例

`llm_llm` 继承 `StackFlow`，实现完整的 LLM 推理节点：

```cpp
class llm_llm : public StackFlow {
    // 重写虚函数:
    setup(work_id, object, data) {
        1. 解析 data (JSON) → 模型名/输入格式/stream 开关
        2. 创建 llm_task 实例 → load_model(config)
        3. 注册 output callback → task_output()
        4. 订阅推理请求 → subscriber_work_id() → task_user_data()
        5. 限制最大并发任务数 (task_count_)
    }

    task_user_data(object, data) {
        可选 stream 解码 (decode_stream) → llm_task::inference(data)
    }

    task_output(data, finish) {
        流式模式: 逐个 delta 发送, finish=true 时发送最后空 delta
        非流式模式: finish 时一次性发送完整结果
    }

    taskinfo(work_id, object, data) {
        work_id 为空: 返回所有任务列表
        work_id 有值: 返回该任务详情 (model/response_format/inputs)
    }
};
```

---

## 五、消息格式规范

所有消息使用 **JSON** 格式：

```json
{
  "request_id": "uuid-xxx",
  "work_id": "llm.0",
  "created": 1719123456,
  "object": "llm.chat.completion",
  "action": "inference",
  "data": { ... },
  "zmq_com": "ipc:///tmp/session.8001.socket",
  "error": {
    "code": 0,
    "message": ""
  }
}
```

**字段说明**：

| 字段 | 说明 |
|------|------|
| `request_id` | 请求追踪 ID，贯穿全链路 |
| `work_id` | 目标推理单元标识：`{unit_name}.{index}`，如 `llm.0`、`asr.3` |
| `action` | 操作类型：`inference` / `setup` / `exit` / `pause` / `taskinfo` |
| `object` | 响应格式标识：`llm.chat.completion` / `stream` 等 |
| `data` | 业务数据负载 |
| `zmq_com` | ZMQ 回推地址（用于异步返回结果） |
| `error.code` | 0 = 成功，负数 = 错误码 |

**错误码速查**：

| 错误码 | 含义 |
|--------|------|
| 0 | 成功 |
| -2 | JSON 格式错误 |
| -4 | 推理数据推送失败 |
| -5 | 模型加载失败 |
| -6 | Unit 不存在 |
| -9 | Unit RPC 调用失败 |
| -11 | 模型运行失败 |
| -18 | Unit 未实现该 action |
| -21 | 任务队列已满 |
| -24 | 推理数据为空 |
| -25 | Stream 数据索引错误 |

---

## 六、与 LLM_Voice_Flow 的本质区别

| 维度 | LLM_Voice_Flow | Edge-LLM-Infra |
|------|---------------|----------------|
| **通信范式** | 硬编码 3 节点直连 | **动态注册/发现** |
| **拓扑** | 固定 ASR→LLM→TTS 管道 | **星型：Unit Manager + N 个 Worker** |
| **节点生命周期** | 手动启动 3 个进程 | **RPC 动态 allocate/release** |
| **消息路由** | 固定端口 6666/6677/7777 | **work_id 寻址 + PUB/SUB 订阅** |
| **扩展性** | 改代码 + 重编译 | **继承 StackFlow + 重写虚函数** |
| **外部接入** | 无（仅 localhost ZMQ） | **TCP Server 桥接** |
| **负载均衡** | 无 | **PUSH/PULL 模式支持多 Worker** |
| **流式输出** | 无 | **内置 stream 编解码 (decode_stream)** |
| **服务发现** | 无 | **RPC list_action 自省** |
| **会话追踪** | 无 | **request_id + work_id 全链路追踪** |

---

## 七、关键技术决策分析

### 7.1 为什么用 ZMQ 而不是 gRPC/HTTP？

| 方案 | 优势 | 劣势 |
|------|------|------|
| **ZMQ (当前)** | 零 broker、极低延迟、IPC 模式性能最优 | 需自行管理 socket 生命周期 |
| gRPC | 自带服务发现、流式传输 | 依赖 protobuf、体积大、边缘设备负担重 |
| HTTP/REST | 通用、易调试 | 轮询开销大、不支持推送 |

ZMQ 的 **IPC (Unix Domain Socket)** 模式在单机多进程场景下延迟最低，适合边缘设备。

### 7.2 为什么用 pthread spinlock 而不是 std::mutex？

```cpp
pthread_spinlock_t key_sql_lock;  // 自旋锁
```

`key_sql` 的读写操作极短（单次 map 查找），spinlock 避免了 mutex 的内核态切换开销。在边缘设备（ARM aarch64）上，这种微优化有意义。

### 7.3 为什么用网络层的 TCP 桥接？

外部客户端（如手机 App、Web 前端）不能直接访问 ZMQ IPC socket。TCP 桥接层将 TCP 连接转换为 ZMQ 消息，实现了**协议转换网关**的功能。

### 7.4 为什么用 eventpp 事件队列？

`eventpp::EventQueue` 提供线程安全的事件分发。RPC 请求线程快速入队后返回，`even_loop` 线程串行处理——避免了业务逻辑加锁的复杂性。

---

## 八、文件职责速查表

```
hybrid-comm/
  include/pzmq.hpp          ZMQ 6模式封装 (PUB/SUB/PUSH/PULL/REQ/REP)
  include/pzmq_data.h       多段消息分段解析 (get_param / set_param)
  src/pzmq_data.cpp         消息内存管理

network/
  include/network/          17个头文件
  src/                      13个实现文件 (.cc)
  核心类:
    TcpServer/TcpClient      服务端/客户端
    EventLoop/Poller         事件循环/epoll
    TcpConnection/Buffer     连接/缓冲区
    Acceptor/Connector       连接建立
    EventLoopThreadPool      多线程模型
    InetAddress/Socket       地址/套接字封装

infra-controller/
  include/StackFlow.h        推理单元基类 (事件驱动+生命周期+RPC注册)
  include/StackFlowUtil.h    工具函数 (JSON解析/work_id编解码/stream解码)
  include/channel.h          推理任务通道 (ZMQ PUB+SUB+PUSH 管理)
  src/StackFlow.cpp          基类实现 (even_loop/setup/exit/pause/register)
  src/channel.cpp            通道实现 (subscriber/send/消息路由)
  src/StackFlowUtil.cpp      工具实现

unit-manager/
  include/all.h              全局宏 (SAFE_READING/SETTING/ERASE)
  include/zmq_bus.h          ZMQ总线: publisher_push / com_send
  include/remote_server.h    系统RPC服务声明
  include/remote_action.h    远程RPC调用声明
  include/unit_data.h        推理单元元数据
  include/session.h          TCP连接的ZMQ桥会话
  src/main.cpp               入口: 加载配置→启动RPC→启动TCP→挂起
  src/config.cpp             读 master_config.json → key_sql
  src/remote_server.cpp      系统RPC实现 (allocate/release/sql)
  src/remote_action.cpp      RPC调用转发 (非inference→call_rpc_action)
  src/zmq_bus.cpp            ZMQ总线实现
  src/tcp_comm.cpp           TCP桥接 (TcpServer+TcpSession)
  src/unit_data.cpp          Unit元数据实现

node/
  llm/README.md              LLM节点说明 (todo)
  test/src/main.cpp          LLM节点示例 (继承StackFlow，完整实现)

sample/
  pub.cc                     ZMQ PUB 使用示例
  sub.cc                     ZMQ SUB 使用示例
  rpc_server.cc              RPC Server 使用示例
  rpc_call.cc                RPC Client 使用示例
  pz_rpc_server.cc           pzmq RPC Server 使用示例
  pz_rpc_call.cc             pzmq RPC Client 使用示例
  stress.py                  压力测试脚本
  test.py                    功能测试脚本

utils/
  json.hpp                   nlohmann/json 单头文件库
  sample_log.h               日志宏 (ALOGD/ALOGI/ALOGW/ALOGE)

docker/                      Docker 部署支持
  build/                     构建脚本
  scripts/                   部署脚本
docker_builder/              Docker 构建配置
```

---

## 九、扩展新推理单元指南

要添加一个新的 AI 推理类型（如 TTS、ASR、CV），只需三步：

### Step 1: 继承 StackFlow

```cpp
#include "StackFlow.h"

class my_tts : public StackFlows::StackFlow {
public:
    my_tts() : StackFlow("tts") {
        // 可选: 设置最大并发任务数
    }

    int setup(const std::string &work_id, const std::string &object,
              const std::string &data) override {
        // 1. 解析配置 JSON
        // 2. 加载 TTS 模型
        // 3. 注册推理回调
        // 4. 订阅推理请求
        auto llm_channel = get_channel(work_id);
        llm_channel->set_output(true);
        llm_channel->subscriber_work_id("",
            std::bind(&my_tts::on_inference, this, ...));
        return 0;
    }

    void on_inference(const std::string &object, const std::string &data) {
        // TTS 推理逻辑
        // 调用 llm_channel->send() 返回结果
    }

    int exit(const std::string &work_id, const std::string &object,
             const std::string &data) override {
        // 卸载模型，清理资源
        return 0;
    }
};
```

### Step 2: 实现 main()

```cpp
int main() {
    signal(SIGINT, sig_handler);
    mkdir("/tmp/llm", 0777);
    my_tts tts_service;
    while (!main_exit_flage) sleep(1);
    return 0;
}
```

### Step 3: 注册到系统

```json
// master_config.json 中添加:
{
  "config_zmq_s_format": "ipc:///tmp/%d.socket",
  "config_zmq_c_format": "ipc:///tmp/%d.socket",
  "config_zmq_min_port": 5000,
  "config_zmq_max_port": 6000,
  "config_tcp_server": 8080
}
```

---

*文档生成时间: 2026-06-30 | 分析范围: 全部 65 个源文件*
