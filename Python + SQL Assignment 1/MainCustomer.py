from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import func

# Create the engine and session
engine = create_engine("postgresql://postgres:password@localhost/postgres")
Session = sessionmaker(bind=engine)
session = Session()

# Create a base class for declarative models
Base = declarative_base()


# Define the Customer model
class Customer(Base):
    __tablename__ = 'customer'
    id = Column(Integer, primary_key=True)
    first_name = Column(String(255))
    last_name = Column(String(255))
    address = Column(String(512))
    dob = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


# Create the table if it doesn't exist
Base.metadata.create_all(engine)

# CRUD operations using SQLAlchemy ORM
def orm_create_customer(first_name, last_name, address, dob):
    customer = Customer(first_name=first_name, last_name=last_name, address=address,
                        dob=dob, updated_at=func.now())
    session.add(customer)
    session.commit()
    return customer.id


def orm_get_customer_by_id(customer_id):
    customer = session.get(Customer, customer_id)
    return customer


def orm_update_customer(customer_id, first_name, last_name, address, dob):
    customer = session.get(Customer, customer_id)
    if customer:
        customer.first_name = first_name
        customer.last_name = last_name
        customer.address = address
        customer.dob = dob
        customer.updated_at = func.now()
        session.commit()


def orm_delete_customer(customer_id):
    customer = session.get(Customer, customer_id)
    if customer:
        session.delete(customer)
        session.commit()


# Retrieve all customers and display them in a formatted manner
def display_customers():
    customers = session.query(Customer).all()
    for customer in customers:
        print(f"Customer ID: {customer.id}")
        print(f"Name: {customer.first_name} {customer.last_name}")
        print(f"Address: {customer.address}")
        print(f"Date of Birth: {customer.dob}")
        print(f"Updated At: {customer.updated_at}")
        print("")



# Example usage of SQLAlchemy ORM
customer_id = orm_create_customer("jader", "belarmino", "Quintas do Sol", '1975-03-07')
print("Created customer (ORM):", customer_id)

customer = orm_get_customer_by_id(customer_id)
print("Retrieved customer (ORM):", customer.id, customer.first_name, customer.last_name)

orm_update_customer(customer_id, "Jader", "Belarmino", "Av do Sol", '1975-03-07')
print("Updated customer (ORM):", customer_id)

orm_delete_customer(customer_id)
print("Deleted customer (ORM)")

customer = orm_get_customer_by_id(customer_id)
if customer:
    print("Retrieved customer (ORM):", customer.id, customer.first_name, customer.last_name, customer.dob,
          customer.updated_at)
else:
    print("NO customer")

display_customers()
