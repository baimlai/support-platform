"""选择性移植 Pi v1.5.0 packages/durable/src/search.ts 的分字段 BM25 排名。

来源提交 f9a26365568127461c47a07a37dcb42c140a3ac5，MIT 许可见 NOTICE.md。
这是纯计算模块：不给工具授权、不修改会话、不读数据库。
差异：Python 标准库没有 Intl.Segmenter；中文按连续词和二字片段匹配，
DBA 中英文同义词由 harness 额外注入，不能声称与上游中文分词完全等价。
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass

STOP_WORDS = frozenset("a an and are as at be by for from in is it of on or that the this to with".split())


def tokenize(text: str) -> list[str]:
    """拆开 snake_case/camelCase，统一英文单复数，并保留中文检索能力。"""
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", text).lower()
    words = re.findall(r"[^\W_]+", text, flags=re.UNICODE)
    result: list[str] = []
    for word in words:
        if word in STOP_WORDS:
            continue
        # 连续中文额外拆成二字片段，例如“查询锁等待”包含“锁等”和“等待”。
        # 真正的领域同义词（如“锁”到 lock）仍由调用方提供。
        if re.fullmatch(r"[\u3400-\u9fff]+", word):
            result.append(word)
            if len(word) > 2:
                result.extend(word[index : index + 2] for index in range(len(word) - 1))
        elif word.isascii():
            if len(word) > 4 and word.endswith("ies"):
                word = word[:-3] + "y"
            elif len(word) > 4 and re.search(r"(ches|shes|sses|xes|zes)$", word):
                word = word[:-2]
            elif len(word) > 3 and word.endswith("s") and not word.endswith("ss"):
                word = word[:-1]
            result.append(word)
        else:
            result.append(word)
    return result


@dataclass(frozen=True, slots=True)
class SearchDocument:
    """给排名器的文档；frozen 防止计算过程中意外修改输入。"""

    name: str
    text: str
    namespace_text: str = ""


@dataclass(frozen=True, slots=True)
class SearchMatch:
    """分数是相对相关性，不是命中概率；covered 是命中的不同查询词数量。"""

    name: str
    score: float
    covered: int
    exact: bool


def _normalize_name(name: str) -> str:
    """与上游一样把非脚本标识符字符换成下划线，兼容 namespace 的 __ 分隔。"""
    return re.sub(r"[^a-z0-9_$]", "_", name.strip().lower())


def rank_documents(
    query: str,
    documents: list[SearchDocument],
    limit: int,
    *,
    extra_terms: set[str] | None = None,
) -> list[SearchMatch]:
    """精确工具名 > 查询词覆盖数量 > 分字段 BM25 分数；同分保留输入顺序。

    额外词是本项目扩展：传英文别名时保留原始 query，精确名称判断才不会失效。
    BM25 的描述词频会饱和，因此重复写一百遍描述也压不过名称中一次命中。
    """
    terms = set(tokenize(query))
    for term in extra_terms or ():
        terms.update(tokenize(term))
    if not terms or not documents or limit <= 0:
        return []
    indexed = [
        (doc, set(tokenize(doc.name)), Counter(tokenize(doc.text)), set(tokenize(doc.namespace_text)))
        for doc in documents
    ]
    average = sum(sum(counts.values()) for _, _, counts, _ in indexed) / len(indexed) or 1
    idf = {}
    for term in terms:
        frequency = sum(
            term in names or term in counts or term in namespace for _, names, counts, namespace in indexed
        )
        idf[term] = math.log(1 + (len(indexed) - frequency + 0.5) / (frequency + 0.5))
    matches = []
    for doc, names, counts, namespace in indexed:
        norm = 1.2 * (1 - 0.75 + 0.75 * sum(counts.values()) / average)
        score = 0.0
        covered = 0
        for term in terms:
            count = counts[term]
            weight = (3 if term in names else 0) + (0.5 if term in namespace else 0)
            weight += count * 2.2 / (count + norm)
            covered += weight > 0
            score += idf[term] * weight
        if score <= 0:
            continue
        local_terms = set(tokenize(doc.name.split("__")[-1]))
        if local_terms and local_terms <= terms:
            score += sum(idf.values())
        exact_query = _normalize_name(query)
        name = _normalize_name(doc.name)
        exact = name == exact_query or name.endswith("__" + exact_query)
        matches.append(SearchMatch(doc.name, score, covered, exact))
    # sorted 是稳定排序：三项键都相同时，沿用目录原顺序，结果可以复现。
    return sorted(matches, key=lambda item: (not item.exact, -item.covered, -item.score))[:limit]
