from typing import Dict, Any, Iterable, Mapping, Sequence, List, Optional, Tuple, Set


def _extract_min_target_map(current_task_row: Mapping[str, Any]) -> Dict[str, Dict[str, float]]:
    """
    Превращаем строку задания в удобную карту:
    {'central': {'min':0.15,'target':0.30}, 'volga': {'min':0.05,'target':0.07}, ...}
    Отсутствующие значения считаем нулями.
    """
    def g(key: str) -> float:
        v = current_task_row.get(key)
        try:
            return float(v) if v is not None else 0.0
        except Exception:
            return 0.0

    return {
        "central":        {"min": g("min_central"),        "target": g("target_central")},
        "north_west":     {"min": g("min_north_west"),     "target": g("target_north_west")},
        "volga":          {"min": g("min_volga"),          "target": g("target_volga")},
        "south":          {"min": g("min_south"),          "target": g("target_south")},
        "urals":          {"min": g("min_urals"),          "target": g("target_urals")},
        "siberia":        {"min": g("min_siberia"),        "target": g("target_siberia")},
        "north_caucasus": {"min": g("min_north_caucasus"), "target": g("target_north_caucasus")},
        "far_east":       {"min": g("min_far_east"),       "target": g("target_far_east")},
    }

def _region_id_to_key(region_id: int) -> str:
    """
    Маппинг ID региона к ключу из задания.
    Подставь реальные соответствия (ниже пример!).
    """
    mapping = {
        1: "central",
        2: "volga",
        3: "urals",
        4: "siberia",
        5: "south",
        6: "north_west",
        7: "north_caucasus",
        8: "far_east",
    }
    return mapping.get(int(region_id), "unknown")



def _build_region_to_warehouses(rows: Iterable[Mapping[str, Any]]) -> Dict[int, List[int]]:
    """
    Превращает результат запроса (wb_office_id, region_id) в карту: region_id -> [warehouse_id...]
    """

    region_to_wh: Dict[int, Set[int]] = {}
    if not rows:
        return {}

    for r in rows:
        if not isinstance(r, Mapping):
            continue
        wh = r.get("wb_office_id")
        reg = r.get("region_id")
        try:
            wh = int(wh)
            reg = int(reg)
        except (TypeError, ValueError):
            continue
        region_to_wh.setdefault(reg, set()).add(wh)

    # приведение к спискам
    return {reg: sorted(list(whs)) for reg, whs in region_to_wh.items()}


def _collect_destination_warehouses_for_plan(
    size_plan_by_region: Dict[int, Dict[int, int]],   # {size_id: {region_id: need_qty}}
    region_to_wh: Dict[int, List[int]],
) -> Dict[int, List[int]]:
    """
    Возвращает карту складов-назначений по регионам, только для тех регионов,
    где есть потребность (need_qty > 0): {region_id: [warehouse_id...]}
    """
    dest_regions: Set[int] = set()
    for _, reg_map in size_plan_by_region.items():
        dest_regions.update(reg_map.keys())

    warehouses_to_ids: Dict[int, List[int]] = {}
    for reg in dest_regions:
        whs = region_to_wh.get(int(reg), [])
        if whs:
            warehouses_to_ids[int(reg)] = whs
    return warehouses_to_ids


def _collect_source_warehouses_for_article(
    product_collection: Dict[int, Dict[str, Any]],
    wb_article_id: int,
) -> Dict[int, List[int]]:
    """
    Источники: для каждого региона собираем список складов, где по этому артикулу есть положительный остаток.
    Формат: {region_id: [warehouse_id...]}
    """
    res: Dict[int, Set[int]] = {}
    art = product_collection.get(int(wb_article_id))
    if not art:
        return {}

    for size_id, size_node in art.get("sizes", {}).items():
        for region_id, region_node in size_node.get("regions", {}).items():
            wh_map: Dict[int, int] = region_node.get("warehouses", {})
            # берем только склады с положительным остатком
            whs = [int(wh) for wh, qty in wh_map.items() if (qty or 0) > 0]
            if not whs:
                continue
            res.setdefault(int(region_id), set()).update(whs)

    return {reg: sorted(list(whs)) for reg, whs in res.items()}