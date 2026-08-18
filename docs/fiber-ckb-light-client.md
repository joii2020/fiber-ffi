# 让 Fiber 跑在客户端：Fiber + CKB Light Client 集成探索

> 状态：文章大纲，待确认后展开。
>
> 定位：介绍一种让 Fiber 与 CKB Light Client 一起运行在客户端上的探索性方案。重点说明思路、实现方式和当前限制，不将其描述为成熟的生产方案。

## 1. 背景：为什么要做这个

- Fiber 对 CKB 全节点 RPC 有哪些依赖；
- 为什么希望 Fiber 能运行在移动端、桌面端等客户端环境中；
- 客户端运行全节点或长期依赖公共 RPC 存在的问题；
- CKB Light Client 为什么可能成为 Fiber 的链上数据来源；
- 本项目的目标是验证技术路径，而不是直接提供生产方案。

## 2. 方案简介

- 在 `fiber-ffi` 中嵌入 CKB Light Client；
- 在 Fiber 和 Light Client 之间增加一个本地 RPC 兼容层；
- Fiber 保持现有调用方式，Light Client 负责同步和验证链上数据；
- 采用“Prepare CKB → Start Fiber”的客户端启动流程；
- 介绍方案的收益，以及首次同步、RPC 能力差异、移动端资源消耗等弊端；
- 受性能影响，目前依旧有部分功能需要使用线上RPC
- 说明当前实现仍存在工程折中，尚未经过完整生产场景验证。

## 3. 具体实现：Fiber 如何连接 Light Client

- `fiber-ffi` 如何启动、持有和停止 Light Client；
- 如何连接 CKB peers、验证链头并同步 Fiber 需要的 scripts；
- 本地 RPC 网关如何把 Light Client 数据转换成 Fiber 需要的 CKB RPC；
- Fiber 启动时如何改用本地网关，以及完整的 Prepare/Start 生命周期；
- 钱包历史扫描起点、数据 readiness、交易验证和广播的处理方式；
- 双方出资时，对端 funding input 未被本地跟踪的问题及当前方案。

## 4. fiber-demo-cli 的使用

- `tools/fiber-demo-cli` 的用途、编译方式和必要配置；
- 如何通过 `rlib` 和 `fiber_ffi::native::FiberNode` 类型化接口嵌入 Fiber；
- 首次运行时如何获取 Funding 地址和发现钱包历史扫描起点；
- 如何 Prepare CKB、观察同步进度并启动 Fiber；
- 如何查询 readiness、余额和节点状态；
- 如何复用已经保存的 Light Client 数据再次启动；
- 这个示例目前能够证明什么、不能证明什么。

## 5. 测试

- 当前单元测试和 E2E smoke test 的覆盖范围；
- 如何使用多个 CKB peers 验证链头同步和脚本扫描；
- 如何证明 Fiber 启动不依赖配置中的完整节点 HTTP RPC；
- 当前已经覆盖的 Prepare/Start/Query/Stop 生命周期；
- 尚待覆盖的通道开关、双方出资、链重组、异常恢复和长期运行测试。

## 6. 遇到的问题与未来优化

### 6.1 当前遇到的问题

- Light Client 不能直接覆盖 Fiber 使用的全部 CKB RPC；
- “数据不存在”和“数据尚未同步”难以用传统 RPC 语义表达；
- 钱包历史扫描时间与安全性的平衡；
- Fiber RPC 超时与 Light Client 按需同步之间的矛盾；
- 双方出资、交易广播状态和客户端生命周期等边界问题。

### 6.2 Fiber 侧可以优化的方向

- 将 CKB 数据访问抽象为可替换的数据源；
- 让 Fiber 能感知 Light Client 的同步进度和 readiness；
- 提供更适合客户端的异步启动和状态反馈；
- 优化双方出资和交易状态相关流程。

### 6.3 CKB Light Client 侧可以优化的方向

- 提供更稳定、易嵌入的 Rust 库接口和生命周期管理；
- 提供更明确的脚本同步范围和 readiness 状态；
- 改进按需数据获取、交易广播和超时控制；
- 优化移动端连接、存储、流量、耗电和后台恢复。

### 6.4 后续工作

- 补充真实通道流程、链重组和异常恢复测试；
- 测量移动端启动时间、存储、流量、内存和耗电；
- 明确哪些问题由适配层解决，哪些应推动 Fiber 或 CKB Light Client 上游改进。
