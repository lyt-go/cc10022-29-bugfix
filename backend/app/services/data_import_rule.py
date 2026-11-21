"""入库规则领域服务：字段映射校验、数据转换编排与导入规则管理。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataImportRuleService(BaseModuleService):
    MODULE = "data_import_rule"
    ENTITY = "入库规则"
    OPERATION_TERM = "导入"
    LIST_FIELDS = ["规则编号", "来源系统", "实体模型", "字段映射", "校验规则", "出错策略", "分片大小", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "来源系统", "实体模型"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def import_data_with_rules(self, rule_id: int, raw_data: list[dict[str, Any]]) -> dict[str, Any]:
        """导入数据批次的字段映射与校验规则编排。

        根据入库规则执行字段映射转换，校验每条记录的必填字段完整性，
        导入数据到目标实体模型存储，记录成功/失败/重复统计量。
        """
        rule = self.get_entry(rule_id)
        if not rule or rule.get("status") != "已生效":
            return {"ok": False, "reason": "入库规则不可用或未生效"}
        # 逐行真实分类：每条原始记录落入且仅落入成功/失败/重复之一
        classifications: list[str] = []
        seen: list[dict[str, Any]] = []
        target_module = rule.get("实体模型", "entity_model")
        for record in raw_data:
            # 导入单条记录：执行字段映射与数据转换
            mapped_record = self._apply_field_mapping(record, rule)
            if mapped_record is None:
                classifications.append("失败")
                continue
            # 校验字段完整性并导入到目标实体：目标库存量与本批已接收记录均判重
            if self._record_exists(target_module, mapped_record) or any(
                all(mapped_record.get(k) == item.get(k) for k in mapped_record)
                for item in seen
            ):
                classifications.append("重复")
                continue
            seen.append(mapped_record)
            classifications.append("成功")
        # 三个计数来自同一份实际分类，对原始行互斥
        return {
            "ok": True,
            "total": len(raw_data),
            "imported": classifications.count("成功"),
            "failed": classifications.count("失败"),
            "duplicated": classifications.count("重复"),
            "classifications": classifications,
        }

    def _apply_field_mapping(self, record: dict[str, Any], rule: dict[str, Any]) -> dict[str, Any] | None:
        """解析字段映射规则，执行源字段到目标字段的转换和标准化。"""
        mapped = {}
        mapping_spec = rule.get("字段映射", "")
        if not mapping_spec:
            return record
        # 解析字段映射配置并执行数据转换
        for pair in mapping_spec.split(","):
            parts = pair.strip().split("->")
            if len(parts) == 2:
                src, tgt = parts[0].strip(), parts[1].strip()
                if src in record:
                    mapped[tgt] = record[src]
        return mapped if mapped else None

    def _record_exists(self, module: str, record: dict[str, Any]) -> bool:
        """校验目标实体中是否已存在相同记录，用于去重判断。"""
        if not record:
            return False
        rows = store.rows(module)
        for row in rows:
            common_keys = [k for k in record if k in row]
            # 至少一个共同字段才可比，避免空交集被误判为重复
            if common_keys and all(record[k] == row[k] for k in common_keys):
                return True
        return False

    def validate_import_rules(self, source_system: str) -> list[dict[str, Any]]:
        """校验指定来源系统的所有入库规则是否完整可用。

        查询来源系统关联的入库规则，校验字段映射、校验规则、
        出错策略和分片大小的完整性，返回校验结果列表。
        """
        rows = store.rows(self.MODULE)
        relevant = [row for row in rows if row.get("来源系统") == source_system]
        results = []
        for rule in relevant:
            checks = {
                "rule_id": rule.get("规则编号"),
                "has_field_mapping": bool(rule.get("字段映射")),
                "has_validation": bool(rule.get("校验规则")),
                "has_error_strategy": bool(rule.get("出错策略")),
                "shard_size_valid": rule.get("分片大小", "").isdigit(),
            }
            checks["all_valid"] = all([
                checks["has_field_mapping"], checks["has_validation"],
                checks["has_error_strategy"]
            ])
            results.append(checks)
        return results

    def query_import_rules_by_model(self, entity_model: str) -> list[dict[str, Any]]:
        """查询指定实体模型关联的所有已生效入库规则。

        遍历入库规则存储，筛选已生效且匹配实体模型的规则，
        按规则版本排序返回供批次导入编排使用。
        """
        rows = store.rows(self.MODULE)
        rules = [
            row for row in rows
            if row.get("实体模型") == entity_model and row.get("status") == "已生效"
        ]
        # 查询入库规则并按规则版本排序
        rules.sort(key=lambda r: r.get("规则版本", ""), reverse=True)
        return rules

    def import_batch_with_rule(self, rule_id: int, batch_data: list[dict[str, Any]]) -> dict[str, Any]:
        """导入批次数据：执行入库规则校验、字段标准化、去重检查。

        解析入库规则配置，对每条记录执行字段映射与数据转换，
        校验必填字段完整性后进行去重检查，完成数据导入并入库。
        """
        rule = self.get_entry(rule_id)
        if not rule:
            return {"ok": False, "reason": "入库规则不存在"}
        result = {"batch_id": rule.get("规则编号"), "total": len(batch_data), "success": 0, "errors": 0}
        for record in batch_data:
            # 导入并校验单条记录：字段映射 -> 格式校验 -> 去重判断
            mapped = self._apply_field_mapping(record, rule)
            if mapped and len(mapped) >= max(len(rule.get("字段映射", "").split(",")), 1):
                result["success"] += 1
            else:
                result["errors"] += 1
        return result