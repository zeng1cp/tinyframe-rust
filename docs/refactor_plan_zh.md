# TinyFrame 设计模式重构建议（no_std / 零运行时开销）

> 目标：将当前 `TinyFrame` 单体职责拆分为可组合协议栈，同时保持 `no_std` 与接近手写路径的性能。

## 1. 对你当前方案的评估

你的方向是对的：

- **责任链（RX）** 和 **装饰器（TX）** 非常契合“按字节流推进状态机”的协议处理。
- **策略模式** 用 trait 做编译期注入，天然适配零开销目标。
- **建造者 + 外观** 可以把复杂泛型藏在类型系统后面，提升可用性。

建议重点改进：

1. 观察者“动态注册/注销”要注意 `no_std`：
   - 若依赖堆分配（`alloc`）会破坏纯 `no_std` 体验；
   - 推荐主路径仍使用**静态容量槽位**（当前数组槽位模型），动态能力做成 feature。

2. 责任链不要引入 trait object：
   - `dyn Handler` 会带来间接调用和潜在代码尺寸不确定；
   - 推荐用**静态组合（泛型元组/嵌套结构）**保持内联。

3. 状态机核心与分发核心要分离：
   - 先把“解析状态推进”与“监听器匹配策略”拆开；
   - 这样可独立验证 parser 行为，不与业务回调耦合。

## 2. 推荐的重构蓝图（按落地优先级）

### 阶段 A：先做“职责切片”，不改变外部 API

把当前 `TinyFrame` 切成 4 个内核：

- `RxParserCore`：仅负责 `accept_byte`/`parse_field_byte` 状态推进与错误。
- `RxDispatchCore`：仅负责 frame -> listeners 的匹配与动作（Close/Renew/Next）。
- `TxCore`：保留发送与 multipart。
- `ObserverStore`：封装 ID/Type/Generic 监听器存储。

`TinyFrame` 先只做 façade 转发，保证对现有调用者无感。

### 阶段 B：引入策略点（先少后多）

建议优先抽 3 个 trait：

1. `IdAllocator`（发送 ID 分配）
2. `DispatchPolicy`（ID 优先 / Type 优先 / 首个命中即停等）
3. `ChecksumPolicy`（已有 `Checksum` trait 可继续沿用）

注意：默认实现必须与当前语义完全一致，先保证回归测试全绿。

### 阶段 C：把 RX/TX 组合能力做成“静态管线”

- RX：`Sof -> Header -> HeadCksum -> Payload -> DataCksum -> Dispatch`
- TX：`EncodeHeader -> WriteHeadCksum -> WritePayload -> WriteDataCksum`

实现形式建议：

- 用泛型链式包装（类似 `Layer<L, Next>`）；
- 或用小型宏展开固定层级；
- 避免 `Vec<Box<dyn ...>>`。

### 阶段 D：Builder + Simple API

- `TinyFrameBuilder` 暴露策略/容量/字段宽度配置。
- `new_simple(...)` 固定默认策略，返回常用类型别名。
- 对外隐藏复杂泛型：`type TinyFrameDefault<...> = TinyFrame<...>`。

## 3. 关键接口建议（示意）

```rust
pub trait IdAllocator {
    fn next_id(&mut self, is_master: bool) -> u32;
}

pub trait DispatchPolicy {
    fn dispatch<C, T, K, const ID: usize, const LEN: usize, const TY: usize>(
        &self,
        ctx: &mut C,
        ch: &mut FrameChannel<'_, T, K, ID, LEN, TY>,
        frame: ReceivedFrame<'_>,
        obs: &mut ObserverStore<C, T, K, ID, LEN, TY>,
    ) -> bool;
}

pub trait RxStage {
    type Next;
    fn on_byte(&mut self, b: u8) -> StageResult<Self::Next>;
}
```

要点：
- trait 只放“变化点”；
- 数据布局（缓冲区、状态字段）尽量在具体结构体中，利于优化器内联。

## 4. 性能与体积的守护线（必须做）

每次阶段重构都跑基准与体积对比：

- 吞吐：`accept_byte` 热路径 cycles/byte
- 代码尺寸：`cargo bloat` 或目标产物 size
- 分支行为：常见帧长度下的分支预测命中情况

建议设“红线”：
- 性能回退 > 3% 或二进制增幅 > 5% 即回滚方案。

## 5. 兼容性与迁移策略

- 保留现有 `TinyFrame::new`，内部转为 builder 构建。
- 老监听器 API 保持；新增策略 API 作为可选增强。
- 分 2~3 个小版本引导迁移，避免一次性 breaking change。

## 6. 你方案里最值得补强的 5 点

1. **把“动态观察者”降级为可选 feature**（默认静态槽位）。
2. **明确 DispatchPolicy 的短路语义**（命中后是否继续通知）。
3. **将 Parser 错误模型独立为可测试单元**（不依赖回调）。
4. **Builder 里做编译期参数校验聚合**（字段宽度、checksum 宽度、容量）。
5. **给每个模式定义“退出机制”**：当抽象带来开销时可退回直连实现。

## 7. 一个可执行的最终方案（建议采用）

- 架构：`Facade(TinyFrame)` + `RxParserCore` + `RxDispatchCore<DispatchPolicy>` + `TxCore<IdAllocator, Checksum>` + `ObserverStore`。
- 组合：默认静态组合（泛型）+ 可选 feature 提供动态注册扩展。
- API：
  - 入门：`TinyFrame::new_simple(...)`
  - 专家：`TinyFrameBuilder::new().id_allocator(...).dispatch_policy(...).build()`
- 保障：保持现有测试不变，再新增“策略等价性测试 + 热路径基准 + size gate”。

这个方案能在不牺牲你“零开销/no_std”目标的前提下，最大化可维护性和可扩展性。
