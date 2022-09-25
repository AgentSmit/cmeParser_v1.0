from datetime import datetime
from peewee import *

class BaseModel(Model):
    class Meta:
        database = SqliteDatabase(".\\data.db")


class SymbolCodes(BaseModel):
    ID = AutoField(primary_key=True)
    CME_CODE = CharField(max_length=6)
    MT4_CODE = CharField(max_length=6)
    Post_Product = TextField()
    Is_Use = BooleanField(default=True)
    class Meta:
        table_name = "Symbol_Codes"

class Spread(BaseModel):
    Id = AutoField(primary_key=True)
    Symbol_Id = IntegerField()
    Time = IntegerField()
    Value = IntegerField()
    class Meta:
        table_name = "Spread"

class Options(BaseModel):
    Id = AutoField(primary_key=True)
    Symbol_Id = IntegerField()
    Time = IntegerField()
    Symbol = CharField(max_length=64,null=True,default='')
    Size = IntegerField(null=True,default=0)
    Trade = DoubleField()
    Type = IntegerField()
    Price = DoubleField()
    class Meta:
        table_name = "Options"
    
class Form_Posts(BaseModel):
    Id = AutoField(primary_key=True)
    Name = CharField(max_length=64)
    Value = TextField(null=True)
    class Meta:
        table_name = "Form_Posts"
