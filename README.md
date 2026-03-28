# porcelain

瓷器拍卖数据搜索项目，当前重点是从雅昌拍卖相关页面抓取索引、详情和图片，并把结果落到本地 SQLite，后续可继续做检索和结构化分析。

## 当前结构

- `artron_scraper5.py`：主抓取脚本，支持索引、详情、图片三种阶段
- `detail_coverage_report.py`：详情字段覆盖率统计
- `crawl_status.py`：查看当前抓取进度和最近错误
- `porcelain_auction.db`：本地 SQLite 数据库
- `images/`：下载下来的图片缓存
- `cookie.txt`：抓取时使用的 Cookie

## 运行前提

- Python 3.10+，并安装 `requests`
- 能访问：
  - `https://artso.artron.net`
  - `https://auction.artron.net`
- `cookie.txt` 中有有效 Cookie；如果文件不存在，脚本会提示输入

安装依赖：

```bash
pip install requests
```

## 抓取模式

主脚本支持四种模式：

```bash
python artron_scraper5.py --mode index
python artron_scraper5.py --mode detail
python artron_scraper5.py --mode images
python artron_scraper5.py --mode full
```

常用参数：

- `--detail-limit 500`：本次最多处理 500 条详情
- `--image-limit 500`：本次最多处理 500 条图片任务
- `--delay 3`：每条请求间隔秒数
- `--download-all-images`：下载一条拍品的全部图片
- `--refresh-detail-done`：连已成功详情也重新抓
- `--keyword xxx`：追加关键词
- `--keywords-file path.txt`：从文件追加关键词
- `--max-pages-per-keyword 120`：索引阶段每个关键词最多翻页数

## 断点续跑

详情抓取默认就是断点续跑：

- `detail_status='done'` 的记录会跳过
- `detail_status='error'` 的记录会优先重试
- `detail_status='pending'` 的记录会继续往后跑

也就是说，如果昨天抓到一半中断，今天直接继续执行：

```bash
python artron_scraper5.py --mode detail --detail-limit 500 --delay 5
```

如果只想把昨天失败的那批先补一轮，也可以直接继续跑同一条命令，因为脚本会先处理 `error` 状态。

## 本次已修复的问题

详情页有些记录缺少 `dynasty`、`description`、`size` 等字段，之前保存到 SQLite 时会因为命名绑定参数缺失而报错，例如：

- `You did not supply a value for binding parameter :dynasty.`
- `You did not supply a value for binding parameter :description.`

现在脚本会在写库前为缺失字段自动补 `None`，这些详情页不会再因为字段不全而整条失败。

## 今天如何重新开启爬虫

建议按这个顺序：

1. 先确认 `cookie.txt` 还是有效的
2. 先小批量重试详情，观察是否稳定
3. 没问题后再放大批量

建议命令：

```bash
python artron_scraper5.py --mode detail --detail-limit 50 --delay 5
python artron_scraper5.py --mode detail --detail-limit 500 --delay 5
```

如果详情稳定，再继续图片：

```bash
python artron_scraper5.py --mode images --image-limit 200 --delay 3
```

开跑前先看状态：

```bash
python crawl_status.py
```

## 查看覆盖率

```bash
python detail_coverage_report.py
```

## 构建搜索清洗数据

在详情还持续抓取时，先使用独立派生表做非破坏性清洗，不直接修改 `auction_records` 原始数据：

```bash
python build_search_dataset.py
```

这个脚本会在 `porcelain_auction.db` 里重建：

- `search_records`：搜索用派生表
- `search_records_ready`：默认过滤掉明显图录/书籍/非瓷器书画噪音后的视图

当前会做的处理包括：

- 规范 `auction_date` 为 `YYYY-MM-DD`
- 归一化 `dynasty`
- 从标题/描述中补充 `vessel_type`、`glaze_color`、`motif`
- 结构化 `provenance`，生成可搜索的来源标签和实体名
- 抽取 `condition_info`，并在搜索层生成 `condition_raw`、`condition_tags`
- 标记明显不适合进入搜索结果的记录，而不是删除原始数据
- 生成 `quality_score`，方便后续前端做排序或低质量结果降权

注意：

- 资料类 / 图录 / 书籍类记录只在搜索清洗层排除，不会从 `auction_records` 原始抓取表删除
- `search_records` 会保留 `provenance_raw`、`provenance_tags`、`provenance_entities`
- `auction_records` 会保留 `condition_info`，`search_records` 会保留 `condition_raw`、`condition_tags`
- 后续前端如果需要做“来源筛选”，优先基于 `provenance_tags`，需要展示细节时再读取 `provenance_raw`
- 后续前端如果需要做“品相筛选”，优先基于 `condition_tags`

建议节奏：

1. 继续跑 `detail`
2. 每隔一段时间执行一次 `python build_search_dataset.py`
3. 后续前端优先查询 `search_records_ready`

## 抽样检查搜索数据

如果你想快速人工抽样，而不是手写 SQL，可以直接运行：

```bash
python sample_search_dataset.py
```

默认会输出 4 组样本：

- `excluded`：被排除的记录
- `top_ready`：可搜索结果里质量分较高的记录
- `missing_dynasty`：朝代仍缺失的记录
- `missing_features`：器型 / 釉色 / 纹饰仍缺失的记录
- `provenance`：已经抽出来源信息的记录，会同时输出整段原始描述、抽取后的来源文本、来源标签和实体
- `condition`：已经抽出品相信息的记录，会同时输出整段原始描述、抽取后的品相文本和品相标签

常用参数：

```bash
python sample_search_dataset.py --limit 20
python sample_search_dataset.py --section excluded
python sample_search_dataset.py --section top_ready --section low_quality --limit 15
```

## Git 约定

以下内容不会提交到 Git：

- `cookie.txt`
- `porcelain_auction.db`
- `images/`

源码仓库只保留脚本和文档。搜索相关的清洗结果写回本地 SQLite，不进 Git。
