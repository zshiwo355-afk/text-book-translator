<div align="center">

# 长文翻译器

**本地书籍与长文翻译工作台：连接你自己的模型，用网页、CLI 或 MCP 翻译 TXT / Markdown。**

可以手动对照检查，也可以让同机 Agent 提交书籍、跟踪进度、恢复任务并领取译稿。

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat-square&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?style=flat-square&logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?style=flat-square&logo=react&logoColor=20232A)
![Vite](https://img.shields.io/badge/Vite-5-646CFF?style=flat-square&logo=vite&logoColor=white)

适合书籍、学习资料、技术文档和双语内容整理；支持术语与风格设置、按段保存和中断续跑。

</div>

> [!IMPORTANT]
> 任务、配置和结果保存在本机；开始翻译后，原文以及已启用的术语、整体要求和风格参考会发送到你配置的模型服务。

## 选择使用入口

| 入口 | 适合的使用方式 |
| --- | --- |
| 网页工作台 | 配置模型与术语，上传书籍，查看原文 / 译文对照并控制进度 |
| CLI | 在终端或脚本中执行翻译，使用 `--resume` 继续未完成任务 |
| Agent + MCP | 让支持本地 stdio MCP 的 Agent 提交、查询、暂停、继续、取消任务并下载译稿；不绑定某个 Agent 产品 |

Hermes 用户也可以使用项目附带的 Skill。网页、MCP 和 Hermes Skill 共用后端任务；CLI 在自身进程中执行翻译，请避免同时用 CLI 与后端运行同一任务。

当前直接接受 TXT / Markdown。PDF、EPUB、DOCX 和扫描书需先转换，项目尚未内置 OCR。

## 为什么用它

| 能力 | 你能得到什么 |
| --- | --- |
| 可配置模型接口 | 在页面填写 URL、API Key 和模型名称，不把应用绑定到单一服务商 |
| 多语言原文 | 模型按提示判断原文语言，目标语言支持中文、日语、韩语或自定义 |
| 长文续跑 | 按段保存进度，支持暂停、继续和失败重试，续跑复用已保存的成功译文 |
| 可控翻译 | 提供忠实直译、自然阅读、专业表达、润色程度和翻译速度设置 |
| 术语与风格 | 逐条添加固定译法、整体要求和风格参考，让长文表达更一致 |
| 对照与导出 | 原文 / 译文双栏检查，可导出译文或双语版 TXT、Markdown |
| Agent 自动执行 | 提供标准 MCP 接口与 Hermes Skill，由 Agent 提交、查进度、续跑并下载译稿 |
| 任务与文件保护 | Agent 重复提交相同内容和参数可复用任务；MCP 限定书籍目录，下载校验大小与 SHA-256，并防止覆盖已有文件 |

## 从原稿到译稿

```mermaid
flowchart LR
    A[TXT / Markdown] --> Entry[网页 / CLI / Agent]
    Entry --> B[自动分段]
    B --> C[术语 + 整体要求 + 风格参考]
    C --> D[兼容 OpenAI Chat Completions 的文本模型]
    D --> E[段落对齐与进度落盘]
    E --> F[译文 / 双语 TXT、Markdown]
```

给 Agent 的典型任务是：“把书籍目录里的 book.md 翻译成简体中文，使用忠实翻译，完成后给我双语 Markdown。”Agent 通过 MCP 检查服务、提交任务、记录任务编号、查询或恢复进度，最后下载译稿。翻译由独立后端执行，关闭 MCP 连接不会取消任务；后端需要保持运行。附件回传取决于 Agent 客户端是否支持。

直接接入 Agent，请看下方的 [MCP 配置与使用说明](#mcp让本地-agent-控制翻译)。首次使用也可以先按快速开始运行网页，完成模型配置。

## 快速开始

环境要求：

- Python 3.10+；MCP 与 Hermes Skill 控制脚本使用 Python 3.11+
- Node.js 18+

先运行 `python3 --version`（Windows 使用 `python --version`）确认版本。如果低于 3.10，请把下面命令中的解释器替换为本机已安装的 `python3.10`、`python3.11` 或更高版本。

### 1. 获取项目

```bash
git clone https://github.com/zshiwo355-afk/text-book-translator.git
cd text-book-translator
```

### 2. 启动后端

macOS / Linux：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
uvicorn backend.app:app --reload --host 127.0.0.1 --port 8000
```

Windows PowerShell：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r backend\requirements.txt
uvicorn backend.app:app --reload --host 127.0.0.1 --port 8000
```

### 3. 启动前端

另开一个终端：

```bash
cd frontend
npm ci
npm run dev
```

打开 [http://127.0.0.1:5173](http://127.0.0.1:5173)。

### 4. 配置模型接口

在页面左侧填写：

1. **接口地址（URL）**：填写服务商提供的接口根地址，通常以 `/v1` 结尾，不要填写完整的 `/chat/completions`。
2. **API Key**：填写该服务的访问密钥。
3. **模型名称**：填写同一服务中的文本模型 ID。

三项必须属于同一个服务。保存只代表写入本机，地址、权限、额度和模型兼容性会在首次翻译时验证。API Key 不会回显到页面。

> 只填写 URL 和 API Key 还不够，模型名称也必须填写。项目不限定模型品牌，但接口必须兼容 OpenAI Chat Completions。

## 使用方法

1. 配置模型接口。
2. 拖入 UTF-8 编码的 `.txt` 或 `.md` 文件。
3. 选择目标语言、表达方式和翻译速度。
4. 按需添加术语、整体翻译要求或风格参考。
5. 开始翻译，在双栏工作区检查原文与译文。
6. 完成、暂停或失败后，导出当前可用结果。

暂停会等待当前翻译批次结束并保存后生效；继续时从未完成位置恢复。更换模型配置后，新的连接会用于之后开始或继续的请求，已经完成的内容不会自动重翻。

## 术语与风格控制

- **术语**：一次添加一个“原文词语 → 固定译法”，适合人名、产品名和专业术语。
- **整体要求**：例如“保持对白自然”“书名保留原文”，启用后随翻译批次发送。
- **风格参考**：提供“原文 → 期望译文”，每个批次最多携带最近启用的 3 条参考。
- **备份导入 / 导出**：页面导出的 `corpus.json` 可用于迁移或恢复语料库。

语料库不会训练模型，只会作为当前翻译请求的上下文。导入功能接受本项目导出的 JSON 备份，不是待翻译书籍或 Excel 词表导入器。

## 导出格式

| 文件 | 内容 |
| --- | --- |
| `原文件名.txt` | 纯译文 TXT |
| `原文件名.md` | 纯译文 Markdown |
| `原文件名_bilingual.txt` | 原文与译文对照 TXT |
| `原文件名_bilingual.md` | 带段落状态的原文与译文对照 Markdown |

任务未全部完成时也可以导出；未完成段落会保留原文并标记状态，便于后续处理。

## 模型兼容说明

项目通过 OpenAI Python SDK 调用 `chat.completions`。可用模型至少需要：

- 提供兼容 OpenAI Chat Completions 的接口；
- 接受 `model`、`messages`、`temperature` 和 `stream` 参数；
- 支持文本对话、多语言和足够的上下文长度；
- 能较稳定地按照提示返回带段落 ID 的结构化内容。

通用对话模型通常可以翻译，但并非所有“兼容接口”或所有模型都适合长文翻译。翻译质量、术语一致性、格式稳定性、速度和费用由你选择的模型服务决定。图片、语音和 Embedding 模型不能直接使用。

## 数据与使用边界

- 页面保存的模型配置位于 `backend/config.local.json`，手工环境变量配置位于根目录 `.env`；两者均已被 Git 忽略。
- 上传文件、任务进度和导出结果默认保存在 `backend/data/`。
- 翻译开始前不会发送文件内容；开始后，相关文本和启用的语料会发送到模型服务。
- 模型服务可能产生费用，请查看对应服务商的计费、额度和隐私政策。
- 当前版本面向本地单人工作流，不是带账号、权限和租户隔离的在线 SaaS。
- 当前上传入口只接受 UTF-8 编码的 TXT 和 Markdown 文件。

<details>
<summary><strong>环境变量配置</strong></summary>

页面配置是推荐方式，也可以复制 `.env.example` 为 `.env` 后手工配置：

```dotenv
VITE_API_BASE=http://127.0.0.1:8000

TOKENHUB_API_KEY=your-api-key
TOKENHUB_BASE_URL=https://api.example.com/v1
TRANSLATION_MODEL=your-model-id
DEFAULT_TARGET_LANGUAGE=简体中文
TRANSLATION_TEMPERATURE=0.1
TRANSLATION_STREAM=false
CHUNK_SIZE_CHARS=3500
MAX_RETRIES=3
```

`TOKENHUB_*` 是当前版本保留的历史变量名，不代表只能使用某个服务商。通过页面保存后，页面中的 URL、API Key 和模型名称优先。

</details>

<details>
<summary><strong>命令行翻译</strong></summary>

CLI 复用同一套模型配置、任务存储和续跑机制：

```bash
source .venv/bin/activate
python cli.py translate \
  --input ./book.txt \
  --target-language 简体中文 \
  --resume
```

</details>

## Hermes Agent 自动翻译

项目附带可直接安装的 Hermes Skill。Hermes 负责接收附件和控制任务，翻译器继续使用页面中配置的模型接口；API Key 不会交给 Skill，也不会出现在命令参数和输出中。任务完成后 Skill 会下载译稿，并在当前 Hermes 渠道支持附件时回传。

```mermaid
flowchart LR
    A[发送 TXT / Markdown 给 Hermes] --> B[book-translator Skill]
    B --> C[本地翻译任务 API]
    C --> D[已配置的翻译模型]
    C --> E[任务进度与结果清单]
    E --> F[下载译稿 / 渠道支持时回传]
```

### 安装 Skill

Web 项目本身支持 Python 3.10+；Hermes Skill 控制脚本需要 Python 3.11+。

macOS / Linux：

```bash
SKILL_HOME="${HERMES_HOME:-$HOME/.hermes}/skills/book-translator"
mkdir -p "$SKILL_HOME"
cp -R integrations/hermes/book-translator/. "$SKILL_HOME/"
```

Windows PowerShell：

```powershell
$hermesRoot = if ($env:HERMES_HOME) { $env:HERMES_HOME } else { Join-Path $HOME ".hermes" }
$skillHome = Join-Path $hermesRoot "skills\book-translator"
New-Item -ItemType Directory -Force $skillHome | Out-Null
Copy-Item -Recurse -Force integrations\hermes\book-translator\* $skillHome
```

然后重启 Hermes，或在 Hermes 中执行 `/reload-skills`。Skill 源码位于 [`integrations/hermes/book-translator`](./integrations/hermes/book-translator/)。

验证本地翻译服务和模型配置：

```bash
python3 "${HERMES_HOME:-$HOME/.hermes}/skills/book-translator/scripts/book_translator.py" preflight
```

Windows PowerShell 使用：

```powershell
$hermesRoot = if ($env:HERMES_HOME) { $env:HERMES_HOME } else { Join-Path $HOME ".hermes" }
python (Join-Path $hermesRoot "skills\book-translator\scripts\book_translator.py") preflight
```

看到 `"ok": true` 和 `"configured": true` 表示 Skill 已能连接本地服务，且模型配置已填写。

用于 Agent 长任务时，后端请以单进程方式启动，不要使用开发环境的 `--reload`：

```bash
source .venv/bin/activate
uvicorn backend.app:app --host 127.0.0.1 --port 8000 --workers 1
```

配置好模型接口后，把 `.txt` 或 `.md` 发给 Hermes，并告诉它：

> 把这本书翻译成简体中文，使用忠实翻译；完成后把双语 Markdown 发给我。

Hermes 会获得一个持久化的 `job_id`。相同文件和相同参数被重复提交时会复用原任务，避免因 Agent 重试重复调用模型；完成后只会下载带文件大小和 SHA-256 校验的结果。任务状态也可以随时查询、暂停、继续或取消。后端重启后，再次提交相同任务时会识别失去执行器的 `running` / `pausing` 状态并继续执行。

> [!WARNING]
> 当前 Agent 接口面向同机、单用户和单 worker 使用，后端必须继续绑定 `127.0.0.1`。接口目前没有公网鉴权，不要直接暴露到局域网或互联网。服务重启后需要 Hermes 重新提交同一任务或执行继续；只查询状态不会恢复任务。无人操作时后端不会自行恢复，也不会因重启自行产生模型费用。

取消是终止操作。相同文件和参数会继续匹配已取消的任务；如果确实需要从头再翻一次，请在页面新建任务，或明确修改翻译参数后再交给 Hermes。

模型连接或语料库内容的后续修改不会改变已经生成的任务身份。同一文件和相同显式参数仍会复用旧任务；需要按新模型或新语料重翻时，请在页面新建任务。

当前 Skill 只接受 TXT 和 Markdown。PDF、EPUB、DOCX 需要先转换成文本或 Markdown；扫描版 PDF 还需要 OCR。

## MCP：让本地 Agent 控制翻译

MCP 入口使用 [官方 Python SDK](https://py.sdk.modelcontextprotocol.io/)，要求 Python 3.11+。第一版采用 **同机 stdio MCP → 已启动的翻译后端 → 已配置的翻译模型**。MCP 不直接运行第二套翻译流水线；关闭 Agent 或 MCP 连接后，独立运行的后端仍会继续任务。电脑休眠、退出后端或关机则会中断执行，恢复后需要 Agent 调用继续。

### 安装和连接

在项目虚拟环境中安装可选依赖：

```bash
python -m pip install -r requirements-mcp.txt
```

先按前文在页面配置模型，再单独启动后端（一个 worker，不使用 `--reload`）：

```bash
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8000 --workers 1
```

准备一个专门存放书籍的现有目录。在支持 stdio MCP 的 Agent 客户端中增加下列服务器配置，把三个绝对路径换成自己的路径；Windows 的 Python 位于 `.venv/Scripts/python.exe`。

```json
{
  "mcpServers": {
    "book-translator": {
      "command": "/absolute/path/text-book-translator/.venv/bin/python",
      "args": [
        "/absolute/path/text-book-translator/mcp_server.py",
        "--workspace", "/absolute/path/books",
        "--api-url", "http://127.0.0.1:8000"
      ]
    }
  }
}
```

以上是通用 JSON 配置示例；具体客户端可能使用不同的配置文件结构。Python 和脚本必须使用绝对路径，无需传模型 API Key。MCP 服务只接受本机 HTTP 后端地址，不能用此配置开放远程访问。后端应始终绑定回环地址。

### 给 Agent 的任务示例

把 UTF-8 的 `book.md` 放进上述书籍目录，告诉 Agent：

> 把书籍目录里的 book.md 翻译成简体中文，使用忠实翻译和稳定速度。先检查服务，再提交任务，记录任务编号并检查进度；遇到失败先查看原因，不要无限重试。完成后下载双语 Markdown，把文件交给我。

如果通过聊天附件传书，Agent 客户端需要先将附件保存到 `--workspace` 指定目录。MCP 本身不会读取聊天附件，也不会替客户端发送文件。

| MCP 工具 | 用途 |
| --- | --- |
| `translator_preflight` | 检查服务和模型是否已配置；不调用模型，也不验证额度和真实连通性 |
| `submit_translation` | 提交或复用任务，立即返回 `job_id`；支持目标语言、表达模式、润色程度、已有语料库及速度 |
| `get_translation_status` | 查看进度、错误、执行器是否存活及结果清单 |
| `pause_translation` | 请求暂停，当前批次保存后生效 |
| `resume_translation` | 继续未完成任务，或恢复后端重启后的任务 |
| `cancel_translation` | 终止任务；相同文件和参数再次提交仍会匹配这个已取消任务 |
| `download_translation` | 校验大小及 SHA-256 后保存译稿，返回本机路径；默认双语 Markdown |

后续操作必须明确传入 `job_id`，不会自动操作“最近一本书”。建议每隔约 10 秒查询状态；等待和断开连接不会取消后台任务。`result_ready` 表示可下载，**只有 `status=completed` 才是处理完成**；暂停、失败、取消的可用结果仍是部分译稿。程序处理完成也不等于经过翻译质量校对。

### 当前边界

- 输入仅限 UTF-8 `.txt` / `.md`，单文件不超过 20 MiB；EPUB、文字 PDF、扫描 PDF 和 DOCX 尚需另行转换。
- 输入与输出路径必须位于 `--workspace` 内，越界路径和指向外部的符号链接会被拒绝。默认下载到该目录的 `outputs/`，已有文件不会覆盖。
- 任务索引写入书籍目录的 `.translator-state/jobs.json`，不修改 Hermes 的默认任务索引；完整进度仍由后端存储。
- 开始与继续翻译会使用现有模型配置并可能产生费用，目前没有整本书的预算硬上限。
- 默认使用稳定速度。已知并发模式在批次存在漏段时可能重新请求同批已译段，尚待逐段保存优化。
- 相同文件与显式参数会复用旧任务。仅更换模型或修改语料内容不会自动重翻；需要明确重翻时请在页面新建任务。
- 当前不支持远程鉴权、书籍 OCR、自动术语提取、预算控制或译文质量审核。

## 技术栈

- 前端：React 18 + Vite 5
- 后端：FastAPI + Pydantic
- 模型调用：OpenAI Python SDK
- 实时状态：Server-Sent Events
- 本地存储：JSON 文件

## 开发检查

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
python -m compileall -q backend tests integrations/hermes/book-translator

cd frontend
npm run build
```

安装可选 MCP 依赖后，运行 `python -m unittest discover -s tests -p 'test_mcp*.py' -v` 可验证真实 stdio 握手、工具调用和文件交付。其中全链路测试连接真实后端与翻译流程，并验证 Agent 断开后后台继续执行；配置和文件全部隔离，模型使用测试替身，不调用付费模型。

## 反馈

如果你遇到模型兼容、长文分段、语料导入或导出问题，欢迎通过 GitHub Issues 提交可复现信息。
