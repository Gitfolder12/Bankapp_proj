from peewee import Model, CharField, IntegerField,DateTimeField,AutoField,DeferredForeignKey,DecimalField,BooleanField
from connection.index import db
from dto.account import AccountCreate


class Account(Model):
      id = AutoField()
      user = DeferredForeignKey('user', on_delete='CASCADE')
      number = IntegerField(unique=True)
      balance = DecimalField(default=0.0, max_digits=10, decimal_places=2)  # Account balance
      active = BooleanField(default=True)  # To check if the account is active or not
      
      class Meta:
            database = db


      @classmethod
      def create_account(cls, account:AccountCreate) -> "Account":
          account_dict = account.model_dump()
          new_account = cls.create(**account_dict)
          return new_account.__data__
     