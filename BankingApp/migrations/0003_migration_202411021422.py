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
class Loan(peewee.Model):
    user = IntegerField()
    amount = DecimalField(auto_round=False, decimal_places=2, max_digits=10, rounding='ROUND_HALF_EVEN')
    interest_rate = DecimalField(auto_round=False, decimal_places=2, max_digits=5, rounding='ROUND_HALF_EVEN')
    term_years = IntegerField()
    date_taken = DateField(default=datetime.datetime.now)
    balance = DecimalField(auto_round=False, decimal_places=2, max_digits=10, rounding='ROUND_HALF_EVEN')
    status = CharField(default='owing', max_length=255)
    class Meta:
        table_name = "loan"


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


def migrate_forward(op, old_orm, new_orm):
    op.create_table(new_orm.loan)
    op.run_data_migration()


def migrate_backward(op, old_orm, new_orm):
    op.run_data_migration()
    op.drop_table(old_orm.loan)
