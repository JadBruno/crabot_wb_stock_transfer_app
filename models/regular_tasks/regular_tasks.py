from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class RegionStock:
    name: str # Название региона
    id: int # Ид
    attribute: str # атрибут для упрощение поиска целевых долей
    target_share: int = 0 # Целевая доля товара в регионе
    min_share: int = 0 # Минимальная доля товара в регионе
    min_qty_fixed: int = 0
    warehouses: List[str] = field(default_factory=list) # Массив всех складов региона
    stock_by_warehouse: List[Dict[str, int]] = field(default_factory=list) # Cток по каждому из складов

    stock_by_region_before: int = 0 # Cколько всего в регионе до распределния
    stock_by_region_after: int = 0 # Cколько всего в регионе после распределения
    target_stock_by_region: int = 0 # Считается по формуле target_share * stock_by_region
    min_stock_by_region: int = 0 # Считается по формуле min_share * stock_by_region
    amount_to_deliver: int = 0 # Считается по формуле target_stock_by_region - stock_by_region
    is_below_min: bool = False # Если stock_by_region < min_stock_by_region и amount_to_deliver больше 0

    # ВАЖНО: раньше здесь был общий словарь для всех объектов
    stocks_to_be_sent_to_warehouse_dict: Dict[str, int] = field(default_factory=dict)


@dataclass
class RegularTaskForSize: # Тут храним задание по размеру
    nmId: int # Артикул продукта
    size: str # размер
    tech_size_id: int # Наш айди размера в бд
    total_stock_for_product: int # Общий сток размера на всех складах и всех регионах

    region_src_sort_order: Dict[int, int] = field(default_factory=dict) # Порядок регионов-источников
    region_dst_sort_order: Dict[int, int] = field(default_factory=dict) # Порядок регионов-получателей

    availability_days_by_warehouse: Dict[str, int] = field(default_factory=dict) # Доступность на складах
    availability_days_by_region: Dict[int, int] = field(default_factory=dict) # Доступность по регионам
    
    orders_by_warehouse: Dict[str, int] = field(default_factory=dict) # Заказы по складам
    orders_by_region: Dict[int, int] = field(default_factory=dict) # Заказы по регионам

    # Регионы — создаём новый словарь для каждого экземпляра
    region_data: Dict[int, RegionStock] = field(default_factory=lambda: {
        1: RegionStock(name="Сибирский", id=1, attribute="siberia"),
        2: RegionStock(name="Южный", id=2, attribute="south"),
        3: RegionStock(name="Центральный", id=3, attribute="central"),
        4: RegionStock(name="Дальневосточный", id=4, attribute="far_east"),
        5: RegionStock(name="Северо-западный", id=5, attribute="north_west"),
        6: RegionStock(name="Северо-Кавказский", id=6, attribute="north_caucasus"),
        7: RegionStock(name="Уральский", id=7, attribute="urals"),
        8: RegionStock(name="Приволжский", id=8, attribute="volga"),
    })
