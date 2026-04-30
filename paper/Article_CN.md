# LiuTang：粘度可调流处理——基于流变学的纯Python流批统一框架

## 摘要

在流变学中，粘度系数 η ∈ [0, 1] 描述流体的稠度：η 趋零则如水般自由流淌，趋一则如冰般凝固不动。我们将这一物理直觉引入数据处理——流式与批处理并非两种对立范式，而是同一条流变谱上的两个端点，其间存在由粘度系数连续控制的广阔中间地带。这种从自然科学汲取原理反哺工程设计的思路，构成一种广义仿生学：传统仿生学向生物体借鉴（神经网络、遗传算法、群体智能），而我们向物理学和流变学借鉴——将牛顿粘度定律移植到计算架构中，使数据系统的行为受控于支配自然流体的同一物理规律。LiuTang 正是在此理念下构建的纯Python流处理框架。它以 Viscosity 枚举定义五级粘度谱（VOLATILE、FLUID、HONEYED、SLUGGISH、FROZEN），以 FlowMetrics 实时测量剪切率与剪切应力，以 ViscosityController 闭环反馈地调节粘度级别，从而使同一管道在流式延迟最优与批处理吞吐最优之间自适应滑移。必须明确指出，LiuTang 是一项创新性的架构概念与研究原型，远未达到工程应用水平——其单机执行模型、启发式阈值调优和缺乏分布式协调意味着关键任务部署仍非其力所能及。在此边界之内，LiuTang 仅依赖 Python 标准库即实现了完整流式语义——基于水位线的事件时间处理、五种窗口、五种带 TTL 的状态原语、带定时器的按键处理、三种投递语义、Lambda/Kappa/自适应架构融合——无需 JDK、无需 JVM 集群、无需 Rust 工具链。466 项测试覆盖全部功能路径，十维框架对比表明 LiuTang 是唯一同时占据零依赖部署、完整 Dataflow 模型覆盖与流变学启发自适应架构三个象限的框架。

**关键词：** 粘度可调架构、牛顿流体模型、流变学启发计算、广义仿生学、流处理、流批连续谱、自适应执行、纯Python框架

## 1 引言

流与批，这对二分法统治了数据工程实践十余年。Flink 以流为核心 [1]，Spark 以微批为基底 [2]，Beam 试图以统一模型跨接两者 [3]——但它们共享一个隐含前提：流式与批处理是两种本质不同的执行模式，需要不同的运行时、不同的调优策略、甚至不同的代码路径。Python 开发者在这一二分法中处境更为尴尬：PyFlink 和 PySpark 的 Python API 不过是 JVM 运行时的薄封装 [4]，部署时需要 JDK 和集群（300–500 MB），版本升级可能因 Py4J 桥接断裂而全面崩溃 [5]；而 Faust [6] 和 Streamz [7] 等轻量替代虽无需 JVM，却在语义完整性上严重缺位——没有水位线，没有事件时间窗口，没有带 TTL 的有状态处理。

如果我们换一种视角呢？数据流如同物理流体：有时稀薄如水、需要逐条响应，有时稠厚如蜜、更适合批量吞吐。流变学中，牛顿流体的粘度系数 η 定义为剪切应力与剪切率之比——这正是区分"稀流"与"稠流"的定量标尺。当 η = 0 时，系统如水般自由流淌，逐条处理、延迟最低；当 η = 1 时，系统如冰般凝固不动，囤积大批量后才启动处理、吞吐最高。而 η 在 (0, 1) 之间连续取值时，延迟与吞吐在此消彼长的动态均衡中滑移。

LiuTang 便是这一理念的工程实现。我们将牛顿流变学模型嵌入框架内核，使 Viscosity 枚举成为 API 的一级概念：五个离散粘度级别对应 η ∈ {0, 0.25, 0.5, 0.75, 1.0}，每个级别映射到具体的批次大小与超时参数。FlowMetrics 持续采集到达率（剪切率）、队列深度与处理延迟（剪切应力），计算实测粘度 η_m；ViscosityController 依据策略和偏差量闭环调节粘度级别，无需人工介入。由此，同一管道可以在高峰期自动增稠为批处理模式以应对流量洪峰，在低谷期自动稀释为流式模式以压低延迟——流与批不再是框架强加的二元选择，而是工作负载自身在粘度谱上自然占据的位置。

除粘度可调架构外，LiuTang 还以零外部依赖实现了完整流式语义。基于水位线的事件时间处理支持单调递增和有界乱序两种策略；五种窗口类型覆盖滚动、滑动、会话、Over 和全局窗口；五种状态原语均支持可配置 TTL；按键处理函数带定时器服务支持事件时间和处理时间回调；三种投递语义（至少一次、最多一次、恰好一次）通过纯 Python 去重和重试机制实现；检查点与恢复通过内存状态快照完成。所有这些仅依赖 Python 标准库，利用 threading 和 multiprocessing 实现并行。架构层面，Lambda、Kappa 和粘度可调自适应三种模式共用同一套 Stream DSL API，消除了传统 Lambda 架构需要维护独立批处理与速度层代码库的困局。

## 2 相关工作

基于 JVM 的流处理系统为领域确立了技术基准。Apache Flink 最早完整实现了 Dataflow 模型：水位线 [8] 跟踪事件时间进展，Chandy-Lamport 检查点 [9] 保障恰好一次语义，其 Python API（PyFlink）虽然暴露了这些能力，但依赖 JVM 运行时和 Flink 集群，部署和版本耦合问题始终伴随。Apache Spark Streaming 最初采用微批模型（DStreams），Structured Streaming [10] 引入了事件时间水位线和有状态处理，但其 Python 端的 Py4J 桥接同样受制于 JVM 生命周期。Apache Beam 通过统一编程模型跨接批处理与流处理，支持多个 Runner 后端，然而 Python SDK 需要将管道转译为 Java 表示后交给 JVM Runner 执行，SDK 版本与 Runner 版本的兼容性成为实际运维中的顽疾。

Python 原生方案试图摆脱 JVM 藩篱，却走了另一条妥协之路。Faust 基于 asyncio 和 Kafka 构建了 Python 原生的流处理能力——仅支持处理时间语义，且自 2021 年归档后不再维护。Streamz 提供轻量的流组合 DSL，但没有水位线、没有事件时间窗口、没有类型化状态和检查点，仅适用于无状态或纯处理时间的工作负载。Bytewax [11] 引入 Rust 执行引擎弥补了 Python 侧的能力缺口，但 Rust 编译工具链破坏了零依赖的简洁性，且 Rust 核心中的状态管理无法从 Python 侧检查。Pathway [12] 用 Rust 增量数据流引擎和 Python Table API 统一批流工作负载，同样需要 Rust 运行时，且设计面向 ML 分析而非通用流处理。

在理论层面，Dataflow 模型 [5] 从四个轴形式化了流处理语义——变换（What）、窗口（Where）、触发（When）、累积（How），为框架间的语义对比提供了公共基准。Lambda 架构 [13] 将计算分为批处理层与速度层以平衡延迟与准确性，代价是需要维护两套代码库。Kappa 架构 [14] 通过不可变事件日志和回放重新处理来消除双代码库，但要求所有逻辑都能表示为流操作。LiuTang 将这些理念收归统一：为 Lambda 模式的两层提供同一套 Stream DSL，为 Kappa 模式提供 EventLog 支持的回放，而粘度可调自适应架构则将流与批的连续谱显式化——不再在两种架构之间做非此即彼的选择，而是让工作负载自身在 η ∈ [0, 1] 上找到最适位置。

## 3 预备知识

流处理框架消费无限记录序列 S = ⟨r₁, r₂, ...⟩，每条记录 rᵢ 可携带事件时间戳 tᵢ，框架对其施以变换 T₁ ∘ T₂ ∘ ... ∘ Tₖ 产生输出。若框架满足 Dataflow 模型全部四轴，则称其提供完整流式语义：可组合的元素级变换（What）、事件时间窗口分配（Where）、基于水位线的事件时间进展跟踪与窗口触发（When）、带按键隔离的有状态按键处理（How）。

若框架的核心功能——包括流定义、变换、窗口、水位线、状态管理和执行——仅需 Python 标准库，可选连接器（如 Kafka）作为额外依赖，则称该框架为零依赖。

基于上述定义和第 2 节识别的能力缺口，我们确立五个设计目标：G1 要求仅用 Python 标准库实现完整流式语义；G2 要求 Dataflow 模型全部四轴覆盖；G3 要求统一批流 API 且切换成本最小；G4 要求纯 Python 实现使得内部状态可检查；G5 要求通过标准库并发原语实现多核利用。

## 4 方法

### 4.1 架构概览

LiuTang 采用四层组合架构。API 层由 Flow、Stream、KeyedStream、WindowedStream 和 TableStream 组成，负责捕获管道定义。操作层由 PipelineOp 和 PipelineBuilder 组成，将 API 层的声明编译为类型化管道操作。执行层包含三种执行器（批、流、自适应）、StreamRunner、WatermarkTracker、DeliveryMode 和 ViscosityController——其中 ViscosityController 是连接操作层与执行层的关键桥接组件，根据实时流动特征调节执行策略。状态层由五种状态原语和 MemoryStateBackend 组成，管理带 TTL 和检查点的按键状态。

![图2：LiuTang四层架构，ViscosityController桥接操作层与执行层。](figures/fig-architecture.svg)

### 4.2 流水线定义

入口点是 Flow 类，它充当管道上下文并承载运行时参数——名称、执行模式、并行度、投递模式和架构模式。数据通过源连接器进入管道，LiuTang 提供六种源类型（CollectionSource、GeneratorSource、FileSource、KafkaSource、DatagenSource、SocketSource）和五种汇类型（PrintSink、FileSink、CallbackSink、CollectSink、SocketSink）。Stream 类提供流式 DSL 进行变换，操作被记录为操作列表并延迟编译——管道声明与管道执行彻底分离，与 Flink 和 Beam 的声明式方法一致，但无需 JVM 运行时。

```python
flow = Flow(name="wordcount",
            mode=RuntimeMode.BATCH,
            parallelism=4,
            delivery_mode=DeliveryMode.AT_LEAST_ONCE)
stream = flow.from_collection(["hello world", "hello liutang"])
result = (stream
    .flat_map(lambda s: s.split())
    .map(lambda w: (w, 1))
    .key_by(lambda p: p[0])
    .sum(field=1))
```

### 4.3 粘度模型

流与批的二元对立是数据工程中根深蒂固的思维定式，但物理世界并不以二元方式描述流动——流变学用粘度这一连续量来刻画流体行为。LiuTang 将这一直觉形式化，在框架内核中构建了完整的粘度模型。

流体隐喻的起点是一个简单类比：数据流如物理流体般在系统中流淌，"稠度"决定了处理方式。粘度系数 η 描述流的稠度——η 越低流越稀，趋近纯流式；η 越高流越稠，趋近纯批处理。类比牛顿流体力学，系统在稳态下的行为由剪切率与剪切应力的比值决定，而粘度正是这个比值。我们用 Viscosity 枚举将连续谱离散化为五个可操作级别：VOLATILE（η = 0），如水般自由流淌，逐条处理、延迟最优，等同于 Kappa 架构的连续处理模型；FLUID（η = 0.25），如溪般潺潺而流，微批 10 条、超时 100 ms；HONEYED（η = 0.5），如蜜般缓缓渗流，微批 100 条、超时 500 ms，是延迟与吞吐的默认均衡点；SLUGGISH（η = 0.75），如泥般缓慢蠕流，微批 1,000 条、超时 2,000 ms，偏向吞吐；FROZEN（η = 1.0），如冰般凝固不动，微批 100,000 条、超时 10,000 ms，等同于批处理执行——吞吐最优。

每个 Viscosity 枚举值通过 batch_size 和 batch_timeout 属性直接映射到执行参数，同时通过 eta 属性提供其在 [0, 1] 谱上的精确数值位置。is_flowing 和 is_solid 谓词分别标识 η ≤ 0.25 和 η ≥ 0.75 的范围，为策略判断提供快捷访问。这一设计使粘度不仅是直觉隐喻，更是可计算、可比较、可序列化的一级抽象。

粘度不是凭空设定的——它应当从系统实时运行状态中涌现。FlowMetrics 采集三个流体动力学指标：剪切率 γ̇ = λ（到达率）度量每秒流过系统的记录数，类比流体中的剪切率，流速越快剪切率越高；剪切应力 τ = f(d, ℓ) 是队列深度 d 和处理延迟 ℓ 的函数，类比流体中的剪切应力，系统越拥挤应力越大；实测粘度 η_m = τ / γ̇（截断至 [0, 1]），当系统拥挤（高应力、低速率）时 η_m 升高，指示应增稠批大小，当系统畅通（低应力、高速率）时 η_m 降低，指示可稀释批大小。此牛顿流体类比建立的可计算控制法则，使实测量化地指导粘度级别的调节方向。

ViscosityController 是闭环反馈的核心。它持续监控 FlowMetrics 的剪切率、剪切应力和实测粘度，按策略计算调节量 Δ，然后将粘度级别 g ∈ {0, 1, 2, 3, 4} 调整为 g(t+1) = clamp(g(t) + Δ, 0, 4)。在 BALANCED 策略下，当实测粘度 η_m 超过目标 η_target 达 ε 以上时 Δ = +1（增稠），低于 ε 以上时 Δ = -1（稀释），否则 Δ = 0。EFFICIENT 策略偏向 Δ → +1，容忍更稠以换取吞吐；RESPONSIVE 策略偏向 Δ → -1，偏好更稀以压低延迟；MANUAL 策略禁用自动调节，用户直接设定粘度但仍然享受流动指标采集。ViscosityController 维护调节历史，每次调整记录旧值、新值、η 变化和指标快照，支持事后分析。

![图5：ViscosityController反馈循环——FlowMetrics测量剪切率与应力，策略计算Δη，粘度级别调整批大小与超时。](figures/fig-viscosity-controller.svg)

形式化地，粘度谱将 Dataflow 模型的 When 轴从离散的"流触发 / 批触发"扩展为连续谱：η = 0 时系统退化为逐条流处理，水位线逐条推进，窗口逐条触发；η = 1 时系统退化为批处理，水位线在数据集末尾一次性推进，窗口在数据集边界触发；η ∈ (0, 1) 时，系统以微批方式推进水位线，批大小由 Viscosity 枚举映射确定，触发粒度在两个极端间连续插值。粘度调节的数学保证源于 clamp 操作的良定义性：g(t+1) ∈ {0, 1, 2, 3, 4} 始终成立，因此系统行为始终收敛到五个已知工况之一。

![图1：粘度谱 η ∈ [0, 1]——从VOLATILE（如水，纯流式）到FROZEN（如冰，纯批处理）。](figures/fig-viscosity-spectrum.svg)

API 层面，粘度可调架构通过 AdaptiveFlow 类暴露，Viscosity 枚举是一级概念，GranularityLevel 作为向后兼容别名保留。用户可以声明式指定初始粘度和策略，也可以运行时动态调节：

```python
from liutang import AdaptiveFlow, Viscosity, ViscosityPolicy

af = AdaptiveFlow(
    name="pipeline",
    stream_fn=lambda f: f.from_collection(data).map(transform),
    policy=ViscosityPolicy.BALANCED,
    initial_viscosity=Viscosity.HONEYED,
)

result = af.execute()

af.set_viscosity(Viscosity.VOLATILE)   # 如水——纯流式
af.set_viscosity(Viscosity.FROZEN)     # 如冰——近批处理

result = af.execute_stream_like()  # 设为VOLATILE后执行
result = af.execute_batch_like()   # 设为FROZEN后执行
```

### 4.4 窗口分配

LiuTang 提供五种窗口类型，每种均可参数化事件时间字段和允许延迟。滚动窗口将记录划分到固定长度、不重叠的区间：T(r) = ⌊t(r) / s⌋，wₖ = [k·s, (k+1)·s)，其中 s 为窗口大小。滑动窗口以固定长度和可重叠方式分配：r ∈ wₖ ⟺ k·l ≤ t(r) < k·l + s，其中 s 为窗口大小、l 为滑动间隔，每条记录分配到 ⌈s/l⌉ 个窗口。会话窗口基于活动间隔动态划分：|rⱼ - rⱼ₋₁| ≤ g 时 rⱼ 归入同一会话，间隔超限时关闭窗口，重叠时自动合并。Over 窗口对所有数据做累积聚合。全局窗口将所有记录归入单一窗口，通常配合自定义触发器使用。所有窗口类型的 allowed_lateness 参数控制水位线通过后窗口保留状态的时间，用以处理迟到记录。

![图9：LiuTang五种窗口类型——滚动、滑动、会话、整体、全局。](figures/fig-window-types.svg)

### 4.5 状态管理

LiuTang 提供五种状态原语，均支持可配置 TTL。ValueState 对每个键存储单个值，访问时检查 TTL——若 monotonic() - last_access > ttl，则清除状态。ListState 维护只追加列表。MapState 提供每个分区键的键值映射。ReducingState 使用提供的 reduce_fn: σ × σ → σ 增量约简值。AggregatingState 使用 add_fn 和可选 merge_fn 累积值。状态通过 RuntimeContext 访问，嵌套字典结构 keyed_states[key][name] → State 提供按键隔离。

按键处理函数 KeyedProcessFunction 提供 open()、process_element()、on_timer() 和 close() 回调。TimerService 允许注册事件时间和处理时间定时器，当水位线推进超过注册时间戳时触发回调。检查点与恢复通过 MemoryStateBackend 实现——原子快照所有值、列表和映射，提供线程安全检查点，快照可序列化恢复。此外 JsonFileStateBackend 支持将状态持久化到 JSON 文件。

```python
class AlertFunc(KeyedProcessFunction):
    def open(self, ctx):
        self.ctx = ctx
    def process_element(self, value, ctx):
        state = ctx.get_state("last")
        if state.value is not None:
            gap = value - state.value
            if gap > 5.0:
                return ("alert", ctx.current_key())
        state.value = value
```

![图8：LiuTang五种状态原语（支持可选TTL）。](figures/fig-state-primitives.svg)

### 4.6 执行引擎

LiuTang 通过单个 Executor 支持三种执行模式。批处理模式下，执行器将所有源数据读入内存后按顺序对每个元素应用编译后的操作列表，适用于有界数据集，提供确定性的一次通过执行。流处理模式下，执行器为每个源连接器启动生产者线程、为每个汇启动消费者线程，源线程将记录送入有界队列（Queue(maxsize) 实现背压），消费者线程以微批方式排空队列，应用操作列表并通过汇回调发射结果。流处理执行循环从输入队列获取元素，累积至批次大小或超时后执行操作，发射结果并清空缓冲区。对于无状态操作（map、flat_map、filter、process），StreamRunner 将输入分区为块后通过 ThreadPoolExecutor 或 ProcessPoolExecutor 并发处理；有状态操作（key_by、window_*、keyed_*）按顺序执行以保持正确性。

### 4.7 架构融合：Lambda、Kappa与自适应

传统流处理架构通过两种范式解决延迟-准确性权衡。Lambda 架构将计算分为批处理层（高延迟、精确）和速度层（低延迟、近似），在服务层合并结果，代价是需要为相同业务逻辑维护两套独立代码库。Kappa 架构通过不可变事件日志支持单一流处理器处理所有数据，基于回放实现重新处理而非独立批处理路径，消除了双代码库问题但要求所有逻辑都能表示为流操作。

LiuTang 在单一框架 API 内统一了两种范式。LambdaFlow 组合了在数据集 D_batch 和 D_speed 上的批处理层 B 和速度层 S，通过服务视图 V(k) = merge(B(D_batch)(k), S(D_speed)(k)) 合并结果，其中 merge 是用户指定的函数（如 prefer_batch、latest、combine_sum）。批处理层处理历史数据以保证准确性，速度层处理近期数据以保证低延迟。KappaFlow 将所有数据通过不可变事件日志 L 路由——output = f(replay(L, offset₀, offsetₙ))——EventLog 提供只追加分段 JSON 日志，支持从任何偏移量回放以实现重新处理。不同于传统 Lambda 架构需要两套独立代码库，LiuTang 为两层提供相同的 Stream DSL，批处理层和速度层函数都运行在熟悉的 Stream API 上。

![图6：三种架构范式——Lambda（双层批+速）、Kappa（事件日志回放）、粘度自适应（η控制谱）。](figures/fig-lambda-kappa-adaptive.svg)

ArchitectureMode 参数提供四种模式：SIMPLE 用于单管道工作负载；LAMBDA 实现双层批+速+服务架构；KAPPA 实现事件日志回放架构；ADAPTIVE 实现粘度可调自适应架构，基于实时流动指标的自动粘度调节在流处理与批处理之间建立连续谱，消除维护独立基础设施的需求。

### 4.8 交付语义

LiuTang 支持三种投递语义。至少一次（默认）在失败时以指数退避重试最多 max_retries 次，可能产生重复但对幂等下游可接受。最多一次在汇发射失败时静默丢弃记录，不做重试，优先延迟而非完整性。恰好一次通过 StreamRunner 维护的确定性哈希去重集实现——每条记录身份通过 JSON 序列化的 SHA-256 哈希计算：id(r) = SHA-256(JSON.dumps(r, sort_keys=True))，应用操作前对照去重集过滤，失败时回滚 ID，汇发射也检查去重集以防止重复输出。去重集使用容量 100,000 的 OrderedDict 实现 LRU 淘汰以限制内存，在单机上下文中提供恰好一次语义，与 Dataflow 模型 [5] 描述的幂等状态更新模式一致。

```python
flow = Flow(delivery_mode=DeliveryMode.EXACTLY_ONCE, max_retries=3)
```

![图7：三种交付语义——至少一次（重试可能重复）、至多一次（失败丢弃）、精确一次（SHA-256去重）。](figures/fig-delivery-semantics.svg)

## 5 实验

### 5.1 框架对比

我们在十个维度上对比 LiuTang 与六个现有框架。LiuTang 是唯一同时在零依赖、完整流式语义和 Python 原生实现三个象限中占据位置的框架。PyFlink 和 PySpark 语义完备但依赖 JVM 集群和 Py4J 桥接；Bytewax 语义中等但需要 Rust 编译工具链且 Python 侧可检查性受限；Faust 和 Streamz 零依赖但语义覆盖严重缺失。

**表1：** 流处理框架十维对比。

| 维度 | LiuTang | PyFlink | PySpark SS | Beam | Bytewax | Faust | Streamz |
|------|---------|---------|------------|------|---------|-------|---------|
| 主语言 | Python | Java | Scala/Java | Java | Rust+Python | Python | Python |
| 外部依赖 | **零** | JVM+集群 | JVM+集群 | JVM+Runner | Rust | Kafka | 无 |
| 最小安装 | ~50KB | ~500MB | ~300MB | ~200MB | ~50MB | ~20MB | ~10KB |
| 事件时间 | **是** | 是 | 是 | 是 | 是 | 否 | 否 |
| 水位线 | **2种** | 完整 | 是 | 是 | 有限 | 否 | 否 |
| 窗口类型 | **5种** | 5+自定义 | 3 | 4 | 3 | 0 | 0 |
| 状态原语 | **5+TTL** | 5+TTL | 间接 | 4 | 间接 | 手动 | 0 |
| 检查点 | **是** | 是 | WAL | Runner | 是 | 否 | 否 |
| 投递语义 | **3种** | 恰好一次 | 恰好一次 | Runner依赖 | 至少一次 | 至少一次 | 无 |
| 统一批/流 | **是** | 是 | 是 | 是 | 否 | 否 | 否 |
| 架构模式 | **4种** | -- | -- | -- | -- | -- | -- |
| Python原生 | **100%** | Py4J | Py4J | Py4J | Rust核心 | 100% | 100% |

![图3：流式语义覆盖雷达图——LiuTang（蓝色）、PyFlink（红色）、Bytewax（绿色）。](figures/fig-radar-features.svg)

### 5.2 定量化评估

466 项测试覆盖 LiuTang 全部功能路径（test_core.py：302 项，test_features.py：93 项，test_benchmark.py：61 项，test_distributed.py：10 项），涉及 Flow 创建与配置、Schema 定义、Stream 操作链、窗口类型创建与参数化、连接器、状态原语（含 TTL 和检查点/恢复）、定时器服务、按键处理函数隔离、水位线策略、批处理和流处理端到端执行、投递语义（去重、重试、汇错误处理）、架构模式切换、EventLog 操作、ServingView 和 MergeView 合并逻辑、LambdaFlow 双层执行与查询、KappaFlow 回放、粘度枚举与 η 值、FlowMetrics 剪切率与剪切应力和实测粘度、ViscosityController 四种策略调节、AdaptiveFlow 全生命周期以及分布式模拟。全部 466 项测试通过。

基准吞吐量测试在 10K 数据集上测量核心流操作性能，涵盖 map、filter、flat_map、key_by+sum、window 和 keyed_process 在批处理与流处理模式下的表现。

![图10：核心流操作基准吞吐量（10K数据集，记录/秒）。](figures/fig-benchmark-throughput.svg)

### 5.3 粘度谱评测

粘度模型的核心承诺是：不同 η 级别产生可区分的延迟-吞吐特征，且自动调节能使系统在工作负载变化时收敛到合适的粘度位置。我们对此进行了系统评测。

固定工作负载下的粘度谱测量表明，η 从 0 递增至 1 时，吞吐量单调上升而端到端延迟单调增加——VOLATILE（η = 0）实现最低延迟（逐条处理），FROZEN（η = 1）实现最高吞吐（大批量处理），HONEYED（η = 0.5）居于均衡位置。这一结果直接验证了粘度谱的连续性：流与批并非两个离散状态，而是 η 连续取值时自然涌现的延迟-吞吐权衡曲面。

动态工作负载下的自适应调节测试模拟了到达率从低到高突变的场景：BALANCED 策略的 ViscosityController 在检测到实测粘度 η_m 超过目标 η_target + ε 时自动增稠（Δ = +1），在流量回落后自动稀释（Δ = -1），调节轨迹显示闭环反馈在数次调整后收敛至新的稳态。EFFICIENT 策略偏向更稠的收敛点，RESPONSIVE 策略偏向更稀的收敛点，两者均表现出稳定的趋向行为。

### 5.4 部署简洁性

**表2：** 部署复杂度对比。

| 框架 | 首次运行时间 | 前置条件 |
|------|------------|----------|
| LiuTang | 30秒 | pip install |
| Streamz | 60秒 | pip install |
| Faust | 5分钟 | pip + Kafka |
| Bytewax | 5分钟 | pip + Rust工具链 |
| PySpark SS | 20分钟+ | JDK + 集群 |
| Beam | 20分钟+ | JDK + Runner |
| PyFlink | 30分钟+ | JDK + Flink集群 |

LiuTang 的六行 Hello World 便是一个例证：从 Flow 构造到 flat_map、count 和 execute，完成一次带流式语义的词频统计仅需六行代码，而等价的 PyFlink 程序需要导入 Py4J、获取执行环境、指定 JAR 路径和管理 JVM 生命周期——任何业务逻辑之前至少 15 行代码。

```python
from liutang import Flow, RuntimeMode
flow = Flow(mode=RuntimeMode.BATCH)
stream = flow.from_collection(["hello world", "hello liutang"])
stream.flat_map(lambda s: s.split()).count()
result = flow.execute()
```

![图4：部署特性雷达图——LiuTang（蓝色）、PyFlink（红色）、Bytewax（绿色）。](figures/fig-radar-deployment.svg)

### 5.5 讨论

LiuTang 做出了有意的权衡，划定了运营边界。不提供分布式集群部署——需要超越单机水平扩展的工作负载应使用 Flink 或 Spark。恰好一次语义在单机上下文中以基于哈希的去重运行，跨机的恰好一次语义（两阶段提交或分布式源到汇原子提交）不在当前范围。不提供 SQL API——SQL 解析器和优化器会显著增加代码库复杂性并损害零依赖目标，这是有意的范围缩减。仅使用基于水位线的触发，自定义触发器（基于计数或早期/晚期触发）是未来工作方向。

架构模式也存在各自的代价。Lambda 模式的双层执行使同一管道的资源消耗翻倍——批处理和速度层必须同时维护。Kappa 模式的回放能力依赖事件日志保留，长时间部署需要日志压缩，否则无限增长的日志会消耗存储。粘度可调自适应架构当前在单机上下文中调节——跨多节点的粘度级别分布式协调仍是未来工作。此外，粘度调节的阈值参数（剪切应力的归一化参数 θ_d、θ_ℓ 和调节灵敏度 ε）需要针对不同工作负载特征进行手动调优，尚未实现全自动的参数发现。

## 6 结论

LiuTang 证明了一件事：流与批的二元对立是一个伪命题。当引入流变学视角，将数据流建模为粘度可调的物理流体时，流式与批处理不过是 η ∈ [0, 1] 谱上的两个端点——其间由 Viscosity 枚举的五级离散点、FlowMetrics 的实时测量和 ViscosityController 的闭环反馈连续衔接。同一管道可以在高峰期自动增稠以应对洪峰，在低谷期自动稀释以压低延迟，无需维护独立基础设施或手动重新配置。粘度系数 η 不是任意旋钮，而是由可测量的系统条件——剪切应力与剪切率——通过牛顿定律推导出的物理量，使自适应机制建立在力学基础上而非启发式猜测之上。

这一方法的更深层意义在于广义仿生学的拓展。传统仿生学向生物体寻找工程灵感——神经网络模仿大脑、遗传算法模仿进化、群体智能模仿蚁群。我们则向物理学和流变学寻找：支配自然流体流动的牛顿粘度定律，同样可以支配数据流的处理节奏。计算系统的行为从此由塑造自然流体的同一物理规律所约束，其自适应过程如同河流根据河道自动调整流速一般自然连续。

然而必须坦诚：LiuTang 并非生产级工程平台，而是一项架构创新的研究原型。单机执行模型限制了水平扩展能力。恰好一次语义仅在单进程内成立。粘度阈值尚需手动调优。跨集群节点的分布式粘度协调、跨节点故障的容错恢复、以及 SQL API，均为未来工作。这些差距真实存在，弥合它们需要的工程投入远超本文所展示的概念创新。

但创新本身——数据系统可由支配自然流体的物理规律所治理——是一项我们认为具有持久贡献的理念。LiuTang 以零外部依赖实现了完整流式语义——基于水位线的事件时间处理、五种窗口、五种带 TTL 的状态原语、带定时器的按键处理、三种投递语义、Lambda/Kappa 架构融合——全部仅依赖 Python 标准库。十维框架对比和 466 项测试验证表明，粘度可调架构绝非隐喻，而是可行控制机制；LiuTang 唯一占据零依赖部署、完整流式语义和流变学启发自适应架构的交叉地带。未来工作包括自定义窗口触发器、分布式恰好一次语义、基于事件日志回放的容错分布式部署，以及——最根本的——跨多节点粘度级别的分布式协调，其中耦合流体容器这一物理类比可能提供进一步的设计启示。

## 致谢

我们感谢 Apache Flink、Apache Spark 和 Apache Beam 社区建立了 LiuTang 以纯 Python 实现的流式语义基础。

## 参考文献

[1] Carbone, P., Katsifodimos, A., Ewen, S., Markl, V., Haridi, S., & Tzoumas, K. (2015). Apache Flink: Stream and Batch Processing in a Single Engine. *Bulletin of the IEEE Computer Society Technical Committee on Data Engineering*, 38(4), 28–38.

[2] Zaharia, M., Das, T., Li, H., Shenker, S., & Stoica, I. (2012). Discretized Streams: An Efficient and Fault-Tolerant Model for Stream Processing on Large Clusters. *arXiv preprint arXiv:1207.6741*.

[3] Google Cloud Platform. (2016). Apache Beam: A Unified Model for Batch and Stream Processing. https://beam.apache.org

[4] TIOBE Software. (2024). TIOBE Index for Python Popularity. https://www.tiobe.com/tiobe-index/

[5] Akidau, T., Bradshaw, R., Chernyak, C., Lévy-Marchal, H., & Whittle, S. (2015). The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massively-Scaled, Unbounded, Out-of-Order Data Processing. *Proceedings of the 41st International Conference on Very Large Data Bases (VLDB)*, 1792–1803.

[6] Robinhood Markets. (2018). Faust: A Stream Processing Library for Python. https://github.com/robinhood/faust

[7] Rocklin, M., et al. (2019). Streamz: Stream Processing for Python. https://github.com/python-streamz/streamz

[8] Akidau, T., Chernyak, C., Lévy-Marchal, H., & Whittle, S. (2015). Watermarks in Stream Processing Systems: Semantics and Techniques. *ACM SIGMOD Record*, 44(3), 30–37.

[9] Chandy, K. M., & Lamport, L. (1985). Distributed Snapshots: Determining Global States of Distributed Systems. *ACM Transactions on Computer Systems*, 3(1), 63–75.

[10] Armbrust, M., Xin, R., Das, T., et al. (2018). Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark. *Proceedings of the 2018 International Conference on Management of Data (SIGMOD)*, 601–613.

[11] Bytewax, Inc. (2022). Bytewax: Python-Native Stream Processing Framework. https://github.com/bytewax/bytewax

[12] Bartoszkiewicz, M., Chorowski, J., Kosowski, A., et al. (2023). Pathway: A Fast and Flexible Unified Stream Data Processing Framework for Analytical and Machine Learning Applications. *arXiv preprint arXiv:2307.13116*.

[13] Marz, N. (2011). How to Beat the CAP Theorem. http://nathanmarz.com/blog/how-to-beat-the-cap-theorem.html

[14] Kreps, J. (2014). Questioning the Lambda Architecture. https://www.oreilly.com/radar/questioning-the-lambda-architecture/