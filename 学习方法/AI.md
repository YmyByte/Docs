## AI
```bash
## 提示词、角色。问题：怎么让模型理解我想要的
prompt 
｜
# 拉数据，调用API
Context RAG、functioncalling、MCP

## SKILL
本质：结构化能力的说明目录，告诉Agent何时触发以及如何使用该能力
方向：多Agent协作，单个Agent跑完即销毁、idea在代码中的description，然后Agent按需加载

搜索是会消耗Token的

Agent.md --> 100行（相当于一个目录）
去哪找更深的信息
- 架构文档在哪
- 设计原则在哪
- 当前执行计划
- ...
- 测试验证 「测试环境最高级为可读」

文档园丁 --> 维护文档的Agent

1、Agent提交完整的计划，等待批准
2、执行完计划后，每个worker交一份交接报告。不是简单的做完了，而是包含工作总结，发现的问题，任何偏离计划的地方

```

