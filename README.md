# Edge-LLM-Infra

> 分布式边缘 LLM 推理微服务框架 | M5Stack Technology | MIT License

## 项目简介

Edge-LLM-Infra 是一个面向边缘设备的**分布式 AI 推理框架**。它将每类 AI 能力（LLM、ASR、TTS、视觉…）封装为可动态注册/销毁的微服务单元（Unit），通过 **ZeroMQ 消息总线 + TCP 桥接** 实现单元间的异步通信和全生命周期管理。

### 核心特性

- **动态服务注册与发现** — Unit 通过 RPC 动态 allocate/release，无需硬编码
- **多模式通信** — PUB/SUB（广播）、PUSH/PULL（负载均衡）、REQ/REP（RPC 同步调用）
- **TCP 协议桥接** — 外部客户端通过 TCP 接入，内部 ZMQ IPC 高性能通信
- **事件驱动架构** — RPC 请求入队即返回，业务线程串行处理，避免锁竞争
- **流式推理支持** — 内置 stream 编解码，支持 token 级增量输出
- **全链路追踪** — request_id + work_id 贯穿每个请求

### 适用场景

- 单机多进程 AI 管道（ASR → LLM → TTS）
- 多机分布式推理集群（Master + N Workers）
- 边缘网关统一接入多种 AI 能力

---

## 快速开始

### 环境依赖

| 依赖 | 版本要求 | 用途 |
|------|---------|------|
| CMake | ≥ 3.10 | 构建系统 |
| GCC | ≥ 8.0 (需 C++17) | 编译 |
| ZeroMQ (libzmq) | ≥ 4.3 | 消息通信 |
| cppzmq | ≥ 4.9 | ZMQ C++ 绑定 |
| simdjson | ≥ 3.0 | 高性能 JSON 解析 |
| nlohmann/json | ≥ 3.10 | JSON 构造 |
| eventpp | 任意版本 | 事件队列 |
| Boost (部分) | any 模块 | TCP 会话上下文 |

### 安装依赖 (Ubuntu/Debian)

```bash
# 基础工具
sudo apt update
sudo apt install -y build-essential cmake git

# ZeroMQ
sudo apt install -y libzmq3-dev

# cppzmq (头文件)
git clone https://github.com/zeromq/cppzmq.git
cd cppzmq && sudo cp zmq.hpp zmq_addon.hpp /usr/local/include/

# simdjson
git clone https://github.com/simdjson/simdjson.git
cd simdjson && mkdir build && cd build
cmake .. && make -j4 && sudo make install

# nlohmann/json
sudo apt install -y nlohmann-json3-dev

# eventpp (头文件)
git clone https://github.com/abeimler/eventpp.git
sudo cp -r eventpp/include/eventpp /usr/local/include/

# Boost
sudo apt install -y libboost-dev
```

### 编译项目

```bash
cd Edge-LLM-Infra-master
mkdir build && cd build
cmake .. && make -j4
```

编译产物：

| 文件 | 说明 |
|------|------|
| `build/unit_manager` | 主服务进程（sys RPC + TCP 桥接） |
| `build/llm_node` | LLM 推理节点示例 |

### 配置文件

在项目根目录创建 `master_config.json`：

```json
{
    "config_zmq_s_format": "ipc:///tmp/%d.socket",
    "config_zmq_c_format": "ipc:///tmp/%d.socket",
    "config_zmq_min_port": 5000,
    "config_zmq_max_port": 6000,
    "config_tcp_server": 8080,
    "config_work_id": 0
}
```

| 配置项 | 说明 |
|--------|------|
| `config_zmq_s_format` | ZMQ 服务端 socket 地址格式（`%d` 替换为端口号） |
| `config_zmq_c_format` | ZMQ 客户端 socket 地址格式 |
| `config_zmq_min_port` | ZMQ 端口池起始值 |
| `config_zmq_max_port` | ZMQ 端口池结束值 |
| `config_tcp_server` | TCP 桥接服务器监听端口 |
| `config_work_id` | work_id 计数器初始值 |

### 启动服务

**第一步：启动 Unit Manager（必须先启动）**

```bash
cd Edge-LLM-Infra-master
./build/unit_manager
```

启动后：
- 注册 sys RPC 服务 → IPC socket: `/tmp/rpc.sys`
- 启动 TCP 桥接服务器 → 监听 `config_tcp_server` 端口
- 主线程进入事件循环

**第二步：启动推理节点**

```bash
# LLM 推理节点（示例）
./build/llm_node
```

**第三步：通过 TCP 客户端测试**

```bash
# 使用 nc 发送 setup 请求
echo '{"request_id":"test1","work_id":"llm","action":"setup","object":"llm.chat.completion","data":{"model":"test","response_format":"llm.chat.completion","enoutput":true,"input":"hello"}}' | nc localhost 8080
```

---

## 架构概览

### 五层架构

```
┌──────────────────────────────────────────┐
│  ⑤ 接入层   │ TCP Server + Session        │  外部请求入口
├──────────────────────────────────────────┤
│  ④ 编排层   │ StackFlow + llm_channel_obj │  Unit 生命周期 + 通信管理
├──────────────────────────────────────────┤
│  ③ 管理层   │ remote_server + unit_data   │  全局资源分配 + 状态存储
├──────────────────────────────────────────┤
│  ② 通信层   │ pzmq + pzmq_data            │  ZMQ 6 模式封装
├──────────────────────────────────────────┤
│  ① 网络层   │ EventLoop + TcpServer       │  Reactor 异步 TCP
└──────────────────────────────────────────┘
```

### 推理请求全链路

```
TCP Client → TcpServer → unit_action_match()
                            │
              ┌─ action="inference" → ZMQ PUB → Unit 推理 → PUB 回传结果
              │
              └─ action≠"inference" → ZMQ REQ → Unit RPC → 事件入队 → 生命周期管理
```

详细架构分析见 [Edge-LLM-Infra-Architecture-Analysis.md](./Edge-LLM-Infra-Architecture-Analysis.md)。

---

## 核心概念

### Unit（推理单元）

每种 AI 能力封装为一个 Unit，由 `unit_name` 标识（如 `llm`、`asr`、`tts`）。每个 Unit 实例有唯一的 `work_id`：`{unit_name}.{index}`（如 `llm.0`）。

### Unit 生命周期

```
  IDLE  ──setup()──→  RUNNING  ──exit()──→  IDLE
    ↑                    │
    └──── pause() ───────┘
```

### ZMQ 通信模式

| 模式 | ZMQ 类型 | 方向 | 使用场景 |
|------|---------|------|---------|
| `inference_url` | PUB/SUB | Mgr→Unit | 推送推理请求给 Unit（一对多） |
| `output_url` | PUB/SUB | Unit→Mgr | Unit 回传推理结果（一对多） |
| `rpc_ctx_` | REP/REQ | Unit↔Mgr | RPC 控制指令（同步请求-响应） |
| `user_chennal_` | PULL | Mgr←Ext | TCP 桥接接收外部请求 |
| `zmq_com` | PUSH | Mgr→Ext | 异步回传结果给 TCP 客户端 |

### 消息格式

所有消息使用 JSON：

```json
{
    "request_id": "uuid-xxx",
    "work_id": "llm.0",
    "created": 1719123456,
    "object": "llm.chat.completion",
    "action": "inference",
    "data": {},
    "zmq_com": "ipc:///tmp/session.8000.socket",
    "error": { "code": 0, "message": "" }
}
```

### RPC Actions

| Action | 方向 | 功能 |
|--------|------|------|
| `setup` | Ext→Unit | 初始化 Unit，加载模型 |
| `exit` | Ext→Unit | 销毁 Unit，释放资源 |
| `pause` | Ext→Unit | 暂停 Unit |
| `taskinfo` | Ext→Unit | 查询 Unit 状态/任务列表 |
| `inference` | Ext→Unit | 推理请求（走 PUB/SUB 通道，非 RPC） |
| `register_unit` | Unit→sys | 向 Manager 注册新 Unit |
| `release_unit` | Unit→sys | 向 Manager 注销 Unit |
| `sql_select/set/unset` | Unit→sys | 读写全局状态 |
| `list_action` | Ext→Unit | 查询 Unit 支持的所有 RPC |

---

## 项目结构

```
Edge-LLM-Infra-master/
│
├── build.sh                     # 构建脚本
├── master_config.json           # 运行时配置（需自行创建）
├── .clang-format                # 代码格式规范
├── .gitignore                   # 排除 build/ install/
│
├── hybrid-comm/                 # ZMQ 通信原语
│   ├── include/
│   │   ├── pzmq.hpp             # ZMQ 6 模式统一封装
│   │   └── pzmq_data.h          # 多段消息解析
│   └── src/pzmq_data.cpp
│
├── network/                     # TCP 网络库（muduo 风格 Reactor）
│   ├── include/network/         # 17 个头文件
│   │   ├── TcpServer.h          # TCP 服务器
│   │   ├── TcpClient.h          # TCP 客户端
│   │   ├── EventLoop.h          # 事件循环
│   │   ├── Poller.h             # epoll 封装
│   │   └── ...                  # Buffer/Channel/Acceptor/Connector 等
│   └── src/                     # 13 个实现文件
│
├── infra-controller/            # 推理单元基类与通道管理
│   ├── include/
│   │   ├── StackFlow.h          # 单元基类（事件驱动 + RPC 注册）
│   │   ├── StackFlowUtil.h      # 工具函数（JSON 解析/work_id/stream）
│   │   └── channel.h            # llm_channel_obj（ZMQ 通道管理）
│   └── src/                     # 实现文件
│
├── unit-manager/                # 全局资源管理器（主服务进程）
│   ├── include/
│   │   ├── all.h                # 全局宏（SAFE_READING/SETTING/ERASE）
│   │   ├── zmq_bus.h            # ZMQ 总线
│   │   ├── remote_server.h      # sys RPC 服务
│   │   ├── remote_action.h      # RPC 调用转发
│   │   ├── unit_data.h          # Unit 元数据
│   │   └── session.h            # TCP 桥接会话
│   └── src/
│       ├── main.cpp             # 入口：加载配置→启动 RPC→启动 TCP→挂起
│       ├── config.cpp           # 读 master_config.json
│       ├── remote_server.cpp    # sys RPC 实现
│       ├── remote_action.cpp    # RPC 转发
│       ├── zmq_bus.cpp          # 总线实现
│       ├── tcp_comm.cpp         # TCP 桥接
│       └── unit_data.cpp        # Unit 元数据
│
├── node/                        # 推理节点实现
│   ├── llm/README.md            # LLM 节点说明
│   └── test/src/main.cpp        # LLM 节点完整示例
│
├── sample/                      # 通信模式使用示例
│   ├── pub.cc                   # ZMQ PUB 示例
│   ├── sub.cc                   # ZMQ SUB 示例
│   ├── rpc_server.cc            # RPC Server 示例
│   ├── rpc_call.cc              # RPC Client 示例
│   ├── stress.py                # 压力测试
│   └── test.py                  # 功能测试
│
├── utils/                       # 工具库
│   ├── json.hpp                 # nlohmann/json 单头文件
│   └── sample_log.h             # 日志宏
│
├── docker/                      # Docker 支持
├── docker_builder/              # Docker 构建配置
├── thirds/                      # 第三方依赖
└── install/lib/                 # 预编译库
```

---

## 开发指南

### 添加新推理类型

继承 `StackFlow`，重写生命周期虚函数：

```cpp
#include "StackFlow.h"

class my_tts : public StackFlows::StackFlow {
public:
    my_tts() : StackFlow("tts") {}

    // 初始化：加载模型 + 注册回调 + 订阅推理请求
    int setup(const std::string &work_id, const std::string &object,
              const std::string &data) override {
        auto ch = get_channel(work_id);
        ch->set_output(true);
        ch->subscriber_work_id("",
            std::bind(&my_tts::on_infer, this,
                      std::placeholders::_1, std::placeholders::_2));
        return 0;
    }

    // 推理回调
    void on_infer(const std::string &object, const std::string &data) {
        // 1. 执行推理
        // 2. ch->send("tts.audio", result, LLM_NO_ERROR, work_id);
    }

    // 销毁：卸载模型 + 取消订阅
    int exit(const std::string &work_id, const std::string &object,
             const std::string &data) override {
        get_channel(work_id)->stop_subscriber("");
        return 0;
    }
};

int main() {
    my_tts tts;
    while (!exit_flag) sleep(1);
    return 0;
}
```

### 错误码规范

| 错误码 | 含义 |
|--------|------|
| `0` | 成功 |
| `-2` | JSON 格式错误 |
| `-4` | 推理数据推送失败 |
| `-5` | 模型加载失败 |
| `-6` | Unit 不存在 |
| `-9` | Unit RPC 调用失败 |
| `-11` | 模型运行失败 |
| `-18` | Unit 未实现该 action |
| `-21` | 任务队列已满 |
| `-24` | 推理数据为空 |

### 日志级别

```cpp
ALOGD("debug message");   // Debug
ALOGI("info message");    // Info
ALOGW("warning message"); // Warning
ALOGE("error message");   // Error
```

---

## 与 LLM_Voice_Flow 的对比

| 维度 | LLM_Voice_Flow | Edge-LLM-Infra |
|------|:-------------:|:-------------:|
| 通信范式 | 硬编码 3 节点直连 | 动态注册/发现 |
| 拓扑结构 | 固定管道 ASR→LLM→TTS | 星型 Manager + N Workers |
| 生命周期管理 | 手动启停进程 | RPC 动态 allocate/release |
| 消息路由 | 固定端口 6666/6677/7777 | work_id 寻址 + PUB/SUB |
| 扩展新能力 | 改代码 + 重编译 | 继承 StackFlow + 重写虚函数 |
| 外部接入 | 仅 localhost ZMQ | TCP Server 桥接 |
| 负载均衡 | 不支持 | PUSH/PULL 多 Worker |
| 流式输出 | 不支持 | 内置 stream 编解码 |
| 会话追踪 | 无 | request_id 全链路 |

---

## 技术栈

| 组件 | 角色 |
|------|------|
| **ZeroMQ** | 消息通信骨干（IPC/TCP） |
| **Reactor (muduo 风格)** | 异步 TCP 网络 |
| **eventpp** | 线程安全事件队列 |
| **simdjson** | 高性能 JSON 解析 |
| **nlohmann/json** | JSON 构造与序列化 |
| **pthread spinlock** | 低延迟状态同步 |
| **Unix Domain Socket** | 单机高性能 IPC |

---

## 相关文档

- [完整架构分析](./Edge-LLM-Infra-Architecture-Analysis.md) — 65 个源文件的深度分析
- [RK3588 语音助手部署指南](../LLM_Voice_Flow-master/RK3588_Edge_Voice_Assistant_Deployment_Guide.md) — 基于此框架的实际部署案例
