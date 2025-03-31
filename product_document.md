# Incrementality Tool 产品文档

## 项目概述
本项目旨在开发一个用于分析广告增量效果的工具，主要功能包括：
1. 快速查询brand_id与brand_name的对应关系
2. 根据brand_id或name查询iRoas, Roas, incremental_factor, spend等指标
3. 提供指标随时间变化的图表

## 技术架构
1. 前端：Vue.js + Vuetify
2. 后端：Flask
3. 数据库：待定
4. 图表库：Chart.js

## Roadmap
### 阶段1：基础功能开发（1周）
- 实现brand_id与brand_name查询
- 实现核心指标数据查询
- 实现基础图表展示

### 阶段2：优化与部署（3天）
- 添加简单的查询条件
- 优化图表展示
- 使用Docker打包部署
