from pydantic import BaseModel
from typing import Optional, List, Any
from datetime import datetime

class ProductSizeInfo(BaseModel):
    size_id: str
    transfer_qty: int
    transfer_qty_left_virtual: int
    transfer_qty_left_real:int
    is_archived: Optional[bool] = False

class ProductToTask(BaseModel):
    product_wb_id: int
    sizes: List[ProductSizeInfo]

class TaskWithProducts(BaseModel):
    task_id: int
    warehouses_from_ids: Optional[Any]  # JSON field: could be list[int], dict, etc.
    warehouses_to_ids: Optional[Any]
    task_status: Optional[int]
    is_archived: Optional[bool]
    task_creation_date: Optional[datetime]
    task_archiving_date: Optional[datetime]
    last_change_date: Optional[datetime]
    products: List[ProductToTask] = []
