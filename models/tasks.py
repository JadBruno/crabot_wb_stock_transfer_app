from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict
from datetime import datetime

class ProductSizeInfo(BaseModel):
    size_id: str
    tech_size_id: int
    transfer_qty: int
    transfer_qty_left_virtual: int
    transfer_qty_left_real: int
    is_archived: Optional[bool] = False

class ProductToTask(BaseModel):
    product_wb_id: int
    sizes: List[ProductSizeInfo]


class TaskWithProducts(BaseModel):
    task_id: int
    warehouses_from_ids: Optional[Any] = None   
    warehouses_to_ids: Optional[Any] = None
    task_status: Optional[int] = None
    is_archived: Optional[bool] = None
    task_creation_date: Optional[datetime] = None
    task_archiving_date: Optional[datetime] = None
    last_change_date: Optional[datetime] = None
    products: List[ProductToTask] = Field(default_factory=list) 
