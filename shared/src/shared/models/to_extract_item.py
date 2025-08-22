from pydantic import BaseModel


class ToExtractItem(BaseModel):
    mfg_etld1: str
