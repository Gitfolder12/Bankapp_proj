# auto-generated snapshot
from peewee import *
import datetime
import peewee


snapshot = Snapshot()


@snapshot.append
class Account(peewee.Model):
    number = IntegerField(unique=True)
    balance = DecimalField(auto_round=False, decimal_places=2, default=0.0, max_digits=10, rounding='ROUND_HALF_EVEN')
    active = BooleanField(default=True)
    user = snapshot.ForeignKeyField(index=True, model='user', on_delete='CASCADE')
    class Meta:
        table_name = "account"


@snapshot.append
class User(peewee.Model):
    firstname = CharField(max_length=255)
    lastname = CharField(max_length=255)
    phone = IntegerField()
    email = CharField(max_length=255, unique=True)
    password = CharField(max_length=255)
    age = IntegerField()
    class Meta:
        table_name = "user"


@snapshot.append
class Transaction(peewee.Model):
    user = snapshot.ForeignKeyField(index=True, model='user', on_delete='CASCADE')
    amount = DecimalField(auto_round=False, decimal_places=2, max_digits=10, rounding='ROUND_HALF_EVEN')
    type = CharField(max_length=50)
    created_at = DateTimeField(default=datetime.datetime.now)
    updated_at = DateTimeField(default=datetime.datetime.now)
    description = TextField(null=True)
    class Meta:
        table_name = "transaction"
        


