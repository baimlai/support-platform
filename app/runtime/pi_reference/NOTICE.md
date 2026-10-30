# Pi 来源与许可证说明

本目录下的 Python 循环是对 Pi Agent 的选择性移植，不是 Pi 官方 Python SDK。

- 源项目：https://github.com/earendil-works/pi
- 参考提交：`babe30a7e6c185346c85859da30b58183e5964f0`（2026-10-29）
- 主要参考文件：
  - `packages/agent/src/agent-loop.ts`
  - `packages/agent/src/types.ts`
  - `packages/agent/src/agent.ts`
  - `packages/ai/src/types.ts`
  - `packages/ai/src/utils/transcript.ts`
  - `packages/ai/src/utils/event-stream.ts`
  - `packages/ai/src/utils/validation.ts`

Pi 使用 MIT 许可证。下面的许可证文本随移植代码一起保留：

```text
MIT License

Copyright (c) 2025 Mario Zechner

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

本项目的 Python 实现做了简化，具体差异见 `docs/pi-mapping.md`。
