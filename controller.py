from pydantic import BaseModel, BaseSettings, Field
from typing import List, Set, Optional


class SnowflakeCred(BaseSettings):
    username: str
    account: str
    db_schema: str 
    warehouse: str
    role: str
    database: str
    authenticator: str ="externalbrowser"

    class Config:
        env_prefix = "snowflake_"
        env_file = "custom.env"
        case_sensitive = False


class SalesforceCred(BaseSettings):
    url: str
    token: str
    
    class Config:
        env_prefix = "sfdc_"
        env_file = "custom.env"
        case_sensitive = False


# class SalesforceCred(BaseSettings):
#     username: str
#     client_id: str
#     client_secret: Optional[str]
#     private_key: Optional[str]
#     environment: str
#     token: Optional[str]
#     base_url: Optional[str]
#     api_version: str
#     client_timeout: int = 10
#     client_connect_timeout: int = 60

#     class Config:
#         env_prefix = "sfdc_"
#         case_sensitive = False


# def to_upper(string: str) -> str:
#     return string.upper()


# class TableObjectField(BaseModel):
#     name: str
#     label: Optional[str]
#     type: str
#     length: Optional[int]
#     precision: Optional[str]
#     scale: Optional[str]
#     compoundFieldName: Optional[str]
#     comment: Optional[str] = Field(None, alias="inlineHelpText")
#     nullable: Optional[str] = Field(True, alias="nillable")

#     class Config:
#         allow_population_by_alias = True
#         allow_population_by_field_name = True
#         alias_generator = to_upper


# class TableObject(BaseModel):
#     name: str
#     label: Optional[str]
#     table_schema: Optional[str] = Field(None, alias="schema")
#     database: Optional[str]
#     fields: List[TableObjectField]
#     system: str

#     def to_field_name_set(self, make_upper: bool = True) -> Set[str]:
#         field_set: Set[str] = set()
#         for field in self.fields:
#             field_set.add(field.name.upper() if make_upper else field.name)
#         return field_set
