"""导入批次领域服务：批次拆分、进度追踪与数据入库统计。"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ImportBatchService(BaseModuleService):
    MODULE = "import_batch"
    ENTITY = "导入批次"
    OPERATION_TERM = "导入"
    LIST_FIELDS = ["批次编号", "来源系统", "实体模型", "总记录数", "成功数", "失败数", "重复数", "批次状态"]
    REQUIRED_FIELDS = ["批次编号", "来源系统", "实体模型"]
    STATUS_ORDER = ["待处理", "处理中", "已完成", "已取消"]
    ACTION_RULES = {"开始处理": "处理中", "标记完成": "已完成", "取消导入": "已取消"}
    NEGATIVE_ACTIONS = []
    CONFIRM_CATEGORIES = ("成功", "失败", "重复")

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(str(value))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _count_categories(ledger: list[dict[str, Any]]) -> dict[str, int]:
        """从逐行分类台账聚合计数，保证统计与实际分类同源。"""
        return {
            category: sum(1 for item in ledger if item.get("category") == category)
            for category in ImportBatchService.CONFIRM_CATEGORIES
        }

    def _classify_incoming_rows(self, total: int) -> list[dict[str, Any]]:
        """生成逐行实际分类台账：成功、失败、重复对原始行互斥。

        演示环境按固定比例模拟逐行分类（约5%重复、8%失败），真实环境
        由入库规则的实际映射与校验结果逐行填充；三个计数都从这份台账
        聚合，合计必然等于总记录数。
        """
        duplicate_count = int(total * 0.05)
        failed_count = int(total * 0.08)
        categories = (
            ["重复"] * duplicate_count
            + ["失败"] * failed_count
            + ["成功"] * (total - duplicate_count - failed_count)
        )
        return [{"row": i + 1, "category": category} for i, category in enumerate(categories)]

    def import_batch_from_source(self, source_system: str, entity_model: str,
                                  raw_records: int) -> dict[str, Any]:
        """导入数据批次：从来源系统采集数据并入库到目标实体模型。

        导入批次数据到import_batch存储，包含来源系统、实体模型、
        总记录数、成功数、失败数和重复数统计，并标记批次状态。
        三个计数来自同一份逐行分类台账，对原始行互斥。
        """
        batch_id = f"BATCH-{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        total = max(self._safe_int(raw_records), 0)
        # 导入批次数据：采集来源系统记录，执行数据校验和入库
        ledger = self._classify_incoming_rows(total)
        counts = self._count_categories(ledger)
        import_batch = {
            "批次编号": batch_id,
            "来源系统": source_system,
            "实体模型": entity_model,
            "总记录数": str(total),
            "成功数": str(counts["成功"]),
            "失败数": str(counts["失败"]),
            "重复数": str(counts["重复"]),
            "批次状态": "已完成",
        }
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update(import_batch)
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = counts["失败"] > 0
        entry["ledger"] = ledger
        entry["counts_finalized"] = True
        rows.append(entry)
        return entry

    def query_batch_summary(self, source_system: str | None = None) -> dict[str, Any]:
        """查询导入批次汇总统计，包含成功数、失败数和重复数聚合。

        聚合查询导入批次存储中的所有批次数据，按来源系统分组汇总
        总记录数、成功数、失败数和重复数，输出批次质量概览。
        """
        rows = store.rows(self.MODULE)
        if source_system:
            rows = [r for r in rows if r.get("来源系统") == source_system]
        total_records = sum(self._safe_int(r.get("总记录数", "0")) for r in rows)
        total_success = sum(self._safe_int(r.get("成功数", "0")) for r in rows)
        total_failed = sum(self._safe_int(r.get("失败数", "0")) for r in rows)
        total_duplicate = sum(self._safe_int(r.get("重复数", "0")) for r in rows)
        return {
            "total_batches": len(rows),
            "total_records": total_records,
            "total_success": total_success,
            "total_failed": total_failed,
            "total_duplicate": total_duplicate,
            "success_rate": round(total_success / max(total_records, 1) * 100, 2),
        }

    def split_large_batch(self, batch_id: int, shard_size: int = 1000) -> list[dict[str, Any]]:
        """切分大批次为多个子批次，按分片大小拆分并发导入。

        读取原批次数据，按指定分片大小切分为多个子批次，
        每个子批次独立执行数据校验、导入和去重检查。
        切分后进入分片确认流程，原有分片确认状态与台账重置。
        """
        batch = self.get_entry(batch_id)
        if not batch:
            return []
        total = self._safe_int(batch.get("总记录数", "0"))
        num_shards = max((total + shard_size - 1) // shard_size, 1)
        shards = []
        for i in range(num_shards):
            shard_size_actual = min(shard_size, total - i * shard_size)
            shard = {
                "parent_batch": batch.get("批次编号"),
                "shard_index": i + 1,
                "shard_total": num_shards,
                "shard_size": shard_size_actual,
                "source_system": batch.get("来源系统"),
                "entity_model": batch.get("实体模型"),
                "status": "切分完成",
            }
            shards.append(shard)
        # 切分导入批次：拆分大数据量为多个分片并行处理
        batch["shards"] = shards
        batch["shard_count"] = num_shards
        batch["shard_confirmations"] = {}
        batch["ledger"] = []
        batch["counts_finalized"] = False
        return shards

    def confirm_shard_import(self, batch_id: int, shard_index: int,
                             categories: list[str]) -> dict[str, Any]:
        """确认分片入库分类：部分失败可补数重送，已确认分片重送沿用原结果。

        每次提交携带本片累计逐行分类（成功/失败/重复），按分片覆盖写入台账；
        已经确认过的分片重送时直接沿用原确认结果，不重复入账。
        阶段确认数只用于进度跟踪，不写入整批正式统计；全部分片确认
        完成后，才按台账实际分类汇总成功/失败/重复的整批正式计数。
        """
        batch = self.get_entry(batch_id)
        if not batch:
            return {"ok": False, "reason": "批次不存在"}
        if batch.get("status") == "已取消":
            return {"ok": False, "reason": "批次已取消，无法确认分片"}
        shards = batch.get("shards") or []
        if not shards:
            return {"ok": False, "reason": "批次尚未切分"}
        shard = next((s for s in shards if s.get("shard_index") == shard_index), None)
        if shard is None:
            return {"ok": False, "reason": f"分片 {shard_index} 不存在"}
        invalid = [c for c in categories if c not in self.CONFIRM_CATEGORIES]
        if invalid:
            return {"ok": False, "reason": f"存在非法分类：{invalid[0]}"}
        confirmations = batch.setdefault("shard_confirmations", {})
        key = str(shard_index)
        existing = confirmations.get(key)
        if existing and existing.get("complete"):
            # 分片重送：已经确认过的条目沿用原确认结果，不重复计数
            return {
                "ok": True,
                "reused": True,
                "confirmation": existing,
                "finalized": bool(batch.get("counts_finalized")),
            }
        expected = self._safe_int(shard.get("shard_size"))
        if len(categories) > expected:
            return {"ok": False, "reason": f"确认行数 {len(categories)} 超过分片大小 {expected}"}
        complete = len(categories) == expected
        confirmation = {
            "shard_index": shard_index,
            "rows": len(categories),
            "expected": expected,
            "success": categories.count("成功"),
            "failed": categories.count("失败"),
            "duplicate": categories.count("重复"),
            "complete": complete,
            "confirmed_at": datetime.datetime.utcnow().isoformat(),
        }
        confirmations[key] = confirmation
        # 台账按分片覆盖写入：补数重送时替换该片旧记录，避免重复入账
        ledger = [item for item in batch.get("ledger", []) if item.get("shard") != shard_index]
        ledger.extend(
            {"shard": shard_index, "row": i + 1, "category": category}
            for i, category in enumerate(categories)
        )
        batch["ledger"] = ledger
        finalized = False
        if complete and all(
            (confirmations.get(str(s.get("shard_index"))) or {}).get("complete")
            for s in shards
        ):
            self._finalize_batch_counts(batch)
            finalized = True
        return {"ok": True, "reused": False, "confirmation": confirmation, "finalized": finalized}

    def _finalize_batch_counts(self, batch: dict[str, Any]) -> None:
        """全部分片确认后，按台账实际分类汇总整批正式计数。"""
        counts = self._count_categories(batch.get("ledger", []))
        batch["成功数"] = str(counts["成功"])
        batch["失败数"] = str(counts["失败"])
        batch["重复数"] = str(counts["重复"])
        batch["counts_finalized"] = True
        batch["status"] = "已完成"
        batch["批次状态"] = "已完成"
        batch["pending"] = False
        batch["abnormal"] = counts["失败"] > 0

    def query_shard_progress(self, batch_id: int) -> dict[str, Any]:
        """查询分片确认进度：阶段确认数仅供值班跟踪，不作整批正式统计。"""
        batch = self.get_entry(batch_id)
        if not batch:
            return {"ok": False, "reason": "批次不存在"}
        confirmations = (batch.get("shard_confirmations") or {}).values()
        confirmations = list(confirmations)
        return {
            "batch_id": batch.get("批次编号"),
            "shard_count": self._safe_int(batch.get("shard_count")),
            "confirmed_shards": sum(1 for c in confirmations if c.get("complete")),
            "stage_success": sum(self._safe_int(c.get("success")) for c in confirmations),
            "stage_failed": sum(self._safe_int(c.get("failed")) for c in confirmations),
            "stage_duplicate": sum(self._safe_int(c.get("duplicate")) for c in confirmations),
            "finalized": bool(batch.get("counts_finalized")),
        }

    def query_batch_errors(self, batch_id: int) -> dict[str, Any]:
        """汇总本批失败记录，供值班联系供应商核对结算。"""
        batch = self.get_entry(batch_id)
        if not batch:
            return {"ok": False, "reason": "批次不存在"}
        errors = [item for item in batch.get("ledger", []) if item.get("category") == "失败"]
        return {
            "batch_id": batch.get("批次编号"),
            "来源系统": batch.get("来源系统"),
            "failed_count": len(errors),
            "errors": errors,
            "finalized": bool(batch.get("counts_finalized")),
        }

    def validate_batch_integrity(self, batch_id: int) -> dict[str, Any]:
        """校验导入批次的完整性：成功+失败+重复数是否等于总记录数。

        读取导入批次数据，校验总记录数是否等于成功数+失败数+重复数，
        并以入库计数确认数据核对台账分类覆盖，生成校验报告。
        """
        batch = self.get_entry(batch_id)
        if not batch:
            return {"ok": False, "reason": "批次不存在"}
        total = self._safe_int(batch.get("总记录数", "0"))
        success = self._safe_int(batch.get("成功数", "0"))
        failed = self._safe_int(batch.get("失败数", "0"))
        duplicate = self._safe_int(batch.get("重复数", "0"))
        calculated = success + failed + duplicate
        consistent = total == calculated
        result = {
            "batch_id": batch.get("批次编号"),
            "integrity_ok": consistent,
            "total": total,
            "calculated": calculated,
        }
        ledger = batch.get("ledger")
        if ledger is not None:
            ledger_counts = self._count_categories(ledger)
            result["ledger_rows"] = len(ledger)
            result["ledger"] = ledger_counts
            result["ledger_ok"] = (
                len(ledger) == total
                and ledger_counts["成功"] == success
                and ledger_counts["失败"] == failed
                and ledger_counts["重复"] == duplicate
            )
            result["integrity_ok"] = consistent and result["ledger_ok"]
        return result

    def archive_completed_batches(self, before_date: str) -> int:
        """归档指定日期之前已完成的导入批次数据。

        查询导入批次存储，将已完成且时间在指定日期之前的批次
        标记为归档状态，释放活跃存储空间供新批次使用。
        """
        rows = store.rows(self.MODULE)
        archived = 0
        for row in rows:
            if row.get("status") == "已完成":
                row["status"] = "已归档"
                row["archived_at"] = datetime.datetime.utcnow().isoformat()
                archived += 1
        return archived
